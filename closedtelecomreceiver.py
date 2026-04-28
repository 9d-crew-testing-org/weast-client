import base64
import os
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import subprocess
import shutil
import sys
import threading
import traceback
import time

import socket
import struct

import ctypes

global API_SERVER, apiKey
API_SERVER = ""
apiKey = ""
APP_VERSION = "1.0.6"
CONFIG_FILE = "config.xml"
IMAGE_TOPIC = "i2/radar"
BUNDLE_TOPIC = "i2/bundle"

CACHE_FILE = "cache"
cached_filenames = set()

cfg_path = r"C:\\Program Files (x86)\\TWC\\i2\\Managed\\Config\\MachineProductCfg.xml"
mqtt_topic = "connection/machineproductcfg"
cache_timestamp = None
cache_lock = threading.Lock()  # Add lock for thread safety

import requests


def check_for_updates(api_url, current_version, api_key):
    try:
        url = f"{api_url}/api/version?apiKey={api_key}"
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            log(f"[-] Version check failed: {r.status_code}", level="DEBUG")
            return

        version_data = r.json()
        latest_version = version_data.get("srv", "")
        if latest_version and latest_version != current_version:
            log(f"[UPDATE] New version available: {latest_version}",
                level="LOG")
            return True
    except Exception as e:
        log(f"[-] Error checking for update: {e}", level="DEBUG")

    return False

isConfigLocal = True
configs_dir = "configs"

from pathlib import Path

def read_cfg_file(path):
    """Reads a file and returns its contents, or an error message wrapped in HTML comment."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        log(f"[-] Error reading config file {path}: {e}", level="DEBUG")
        return f"<!-- Error reading file: {e} -->"

def send_cfg(client, apiServer, key):
    global cache_timestamp

    try:
        with cache_lock:  # Thread-safe access
            configs_path = Path(configs_dir)
            cfg_file_path = Path(cfg_path)

            # Check for XML files in configs_dir
            if configs_path.exists() and configs_path.is_dir():
                xml_files = [f for f in configs_path.glob("*.xml") if f.is_file()]
                
                if xml_files:
                    xml_files.sort(key=lambda f: f.name)
                    for f in xml_files[:5]:  # Limit to 5 files
                        file_data = read_cfg_file(f)
                        payload = {
                            "timestamp": int(time.time()),
                            "filename": f.name,
                            "data": file_data
                        }
                        try:
                            client.publish(mqtt_topic, json.dumps(payload))
                        except Exception as e:
                            log(f"[-] Failed to publish config {f.name}: {e}", level="DEBUG")
                    # Update cache timestamp to now after sending all files
                    cache_timestamp = int(time.time())
                    return

            # Fallback: send local config file if it exists
            if cfg_file_path.exists():
                try:
                    mod_time = cfg_file_path.stat().st_mtime
                    if mod_time != cache_timestamp:
                        data = read_cfg_file(cfg_file_path)
                        payload = {
                            "timestamp": int(time.time()),
                            "filename": cfg_file_path.name,
                            "data": data
                        }
                        client.publish(mqtt_topic, json.dumps(payload))
                        cache_timestamp = mod_time
                except Exception as e:
                    log(f"[-] Failed to send local config: {e}", level="DEBUG")
    except Exception as e:
        log(f"[-] Error in send_cfg: {e}", level="DEBUG")

def update_check_loop(apiServer, key):
    while True:
        try:
            if check_for_updates(apiServer, APP_VERSION, key):
                log("[UPDATE] Downloading new version...", level="LOG")
                download_and_launch_updater(apiServer, key)
                sys.exit(0)
        except Exception as e:
            log(f"[-] Error in update check loop: {e}", level="DEBUG")
        time.sleep(300)


def monitor_cfg_file(client, apiServer, key):
    while True:
        try:
            time.sleep(2)
            send_cfg(client, apiServer, key)
            time.sleep(8)
        except Exception as e:
            log(f"[-] Error in monitor_cfg_file: {e}", level="DEBUG")
            time.sleep(10)  # Wait before retrying


def keepalive_cfg(client):
    while True:
        try:
            data = ""

            if isConfigLocal:
                if os.path.exists(cfg_path):
                    data = read_cfg_file(cfg_path)
            else:
                os.makedirs(configs_dir, exist_ok=True)
                files = [f for f in Path(configs_dir).glob("*.xml") if f.is_file()]
                files.sort(key=lambda f: f.name)
                files = files[:5]

                for f in files:
                    file_data = read_cfg_file(f)
                    data += f"\n<!-- {f.name} -->\n{file_data}\n"

            if data.strip():
                payload = {
                    "timestamp": int(time.time()),
                    "data": data.strip()
                }
                try:
                    client.publish(mqtt_topic, json.dumps(payload))
                except Exception as e:
                    log(f"[-] Failed to publish keepalive config: {e}", level="DEBUG")
        except Exception as e:
            log(f"[-] Error in keepalive_cfg: {e}", level="DEBUG")
        
        time.sleep(300)  # wait 5 minutes

def get_lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '192.168.0.240'
    finally:
        s.close()
    return ip



def load_cache():
    global cached_filenames
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                cached_filenames = set(line.strip() for line in f if line.strip())
    except Exception as e:
        log(f"[-] Error loading cache: {e}", level="DEBUG")
        cached_filenames = set()


def add_to_cache(filename):
    if filename not in cached_filenames:
        cached_filenames.add(filename)
        try:
            with open(CACHE_FILE, "a", encoding="utf-8") as f:
                f.write(filename + "\n")
            log(f"[CACHE] Added {filename} to cache", level="DEBUG")
        except Exception as e:
            log(f"[-] Failed to write to cache file: {e}", level="DEBUG")


def is_in_cache(filename, cache_path="cache"):
    try:
        if not os.path.exists(cache_path):
            return False

        with open(cache_path, "r") as f:
            cached_files = {line.strip() for line in f if line.strip()}

        return filename in cached_files
    except Exception as e:
        log(f"[-] Error checking cache: {e}", level="DEBUG")
        return False


LOG_LEVELS = {
    "DEBUG": 0,
    "LOG": 1,
}

current_log_level = LOG_LEVELS["DEBUG"]


def log(msg, level="DEBUG"):
    try:
        level = level.upper()
        if level not in LOG_LEVELS:
            level = "DEBUG"
        if LOG_LEVELS[level] >= current_log_level:
            print(msg)
    except:
        pass  # Fail silently if logging fails


def create_default_config():
    default_xml = """<?xml version="1.0" encoding="UTF-8"?>
<mqtt>
    <server>mqtt.example.com</server>
    <port>1883</port>
    <apiKey>YOUR_API_KEY_HERE</apiKey>
    <topics>
        <topic>i2/radar</topic>
        <topic>i2/data</topic>
		<topic>i2/heartbeat</topic>
    </topics>
    <tls>False</tls>
	<enableUDP>False</enableUDP>
    <multicastIf>127.0.0.1</multicastIf>
	<udpAddress>224.1.1.77</udpAddress>
    <logLevel>DEBUG</logLevel>
</mqtt>
"""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        f.write(default_xml)
    print(
        f"[CONFIG] {CONFIG_FILE} created with default values. Please edit it and rerun."
    )
    sys.exit(0)

def parse_bool(value, default=True):
    if value is None:
        return default
    return str(value).strip().lower() == "true"

def load_config():
    if not os.path.exists(CONFIG_FILE):
        create_default_config()

    tree = ET.parse(CONFIG_FILE)
    root = tree.getroot()

    if root.tag != "mqtt":
        print(f"[-] Invalid {CONFIG_FILE}: root element is not <mqtt>")
        sys.exit(1)

    server = root.findtext("server", default="mqtt.example.com")
    port = int(root.findtext("port", default="1883"))
    tls = parse_bool(root.findtext("tls"), default=True)
    isConfigLocal = parse_bool(root.findtext("localConfig"), default=True)
    apiKey = root.findtext("apiKey", default="")
    udp = parse_bool(root.findtext("enableUDP"), default=False)
    udpAddr = root.findtext("udpAddress", default="224.1.1.77")
    mcastIf = root.findtext("multicastIf") or "127.0.0.1"

    log_level_node = root.find("logLevel")
    if log_level_node is not None:
        level = log_level_node.text.strip().upper()
        if level in LOG_LEVELS:
            current_log_level = LOG_LEVELS[level]

    topics_node = root.find("topics")
    topics = [t.text for t in topics_node.findall("topic")] if topics_node is not None else []

    config = {
        "server": server,
        "port": port,
        "apiKey": apiKey,
        "topics": topics,
        "tls": tls,
        "udp": udp,
        "udpAddr": udpAddr,
        "MCAST_GRP": udpAddr,
        "MCAST_IF": mcastIf,
        "BUF_SIZE": 1396,
        "MULTICAST_TTL": 255
    }

    return config

config = load_config()
BUF_SIZE = 1396
MULTICAST_TTL = 2
conn = None  # Initialize as None

if config["udp"]:
    try:
        MCAST_GRP = config["MCAST_GRP"]
        MCAST_IF = config["MCAST_IF"]
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        conn.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, MULTICAST_TTL)
        conn.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(MCAST_IF))
        conn.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(MCAST_GRP) + socket.inet_aton(MCAST_IF)) 
        print("[+] Multicast socket setup complete.")
        print("MCAST_IF:", config["MCAST_IF"], "| MCAST_GRP:", config["MCAST_GRP"])
    except Exception as e:
        print(f"[-] Failed to setup UDP multicast: {e}")
        conn = None
else:
    print("[-] UDP Multicast is disabled in the config.")


def prettify_xml(elem):
    from xml.dom import minidom
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent=" ")


def handle_work_request(command):
    exec_path = r"C:/Program Files (x86)/TWC/i2/exec.exe"
    try:
        full_cmd = [exec_path, "-async", command]
        subprocess.Popen(full_cmd,
                         shell=False,
                         stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)
        log(f"[WORK] Executed: {command}", level="DEBUG")
    except Exception as e:
        log(f"[-] Failed to execute work request: {e}", level="DEBUG")


def on_connect(client, userdata, flags, reasonCode, properties=None):
    try:
        log(f"[+] Connected to MQTT broker with code {reasonCode}", level="LOG")
        
        if userdata and "topics" in userdata:
            for topic in userdata["topics"]:
                try:
                    client.subscribe(topic)
                    log(f"[SUB] Subscribed to topic: {topic}", level="DEBUG")
                except Exception as e:
                    log(f"[-] Failed to subscribe to {topic}: {e}", level="DEBUG")
        
        try:
            send_cfg(client, API_SERVER, apiKey)
        except Exception as e:
            log(f"[-] Failed to send config on connect: {e}", level="DEBUG")
        
        # Only start threads once
        if not hasattr(client, '_threads_started'):
            client._threads_started = True
            threading.Thread(target=monitor_cfg_file,
                            args=(client, API_SERVER, apiKey),
                            daemon=True).start()
            threading.Thread(target=keepalive_cfg, args=(client, ),
                            daemon=True).start()
            threading.Thread(target=hourly_resubscribe_loop,
                            args=(client, userdata["topics"]),
                            daemon=True).start()
    except Exception as e:
        log(f"[-] Error in on_connect: {e}", level="DEBUG")


def on_disconnect(client, userdata, rc, ok=None, okagain=None):
    try:
        log(f"[-] Disconnected from MQTT broker with code {rc}", level="LOG")
        os.system("title ClosedTelecom Receiver - v1.1.1 (disconnected)")

        if rc != 0:
            reconnect_attempts = 0
            max_attempts = 10
            
            while reconnect_attempts < max_attempts:
                try:
                    reconnect_attempts += 1
                    os.system("title ClosedTelecom Receiver - v1.1.1 (reconnecting...)")
                    log(f"[RECONNECT] Attempt {reconnect_attempts}/{max_attempts}...", level="LOG")
                    
                    client.reconnect()
                    
                    # Resubscribe on successful reconnect
                    if userdata and "topics" in userdata:
                        time.sleep(1)  # Give connection time to stabilize
                        for topic in userdata["topics"]:
                            try:
                                client.subscribe(topic)
                                log(f"[RESUB] Re-subscribed to topic: {topic}", level="DEBUG")
                            except Exception as e:
                                log(f"[-] Failed to resubscribe to {topic}: {e}", level="DEBUG")

                    os.system("title ClosedTelecom Receiver - v1.1.1 (connected)")
                    break
                    
                except Exception as e:
                    log(f"[-] Reconnect attempt {reconnect_attempts} failed: {e}", level="LOG")
                    if reconnect_attempts < max_attempts:
                        time.sleep(min(5 * reconnect_attempts, 30))  # Exponential backoff
                    else:
                        log("[-] Max reconnection attempts reached. Exiting.", level="LOG")
                        sys.exit(1)
    except Exception as e:
        log(f"[-] Critical error in on_disconnect: {e}", level="LOG")


def download_and_launch_updater(api_server, api_key):
    try:
        os.system("title ClosedTelecom Receiver - v1.1.1 (downloading update...)")
        update_dir = os.path.join(".", "temp", "update")
        os.makedirs(update_dir, exist_ok=True)

        updater_url = f"{api_server}/api/latest/updater.exe?apiKey={api_key}"
        updater_path = os.path.join(update_dir, "updater.exe")

        encoder_url = f"{api_server}/api/latest/encoder.exe?apiKey={api_key}"
        encoder_path = os.path.join(update_dir, "encoder_update.exe")

        r = requests.get(updater_url, stream=True, timeout=10)
        if r.status_code != 200:
            log(f"[-] Failed to download updater: HTTP {r.status_code}", level="LOG")
            return

        with open(updater_path, "wb") as f:
            for chunk in r.iter_content(1024 * 64):
                f.write(chunk)

        log(f"[UPDATE] Downloaded updater to {updater_path}", level="DEBUG")

        r2 = requests.get(encoder_url, stream=True, timeout=10)
        if r2.status_code != 200:
            log(f"[-] Failed to download encoder update: HTTP {r2.status_code}", level="LOG")
            return

        with open(encoder_path, "wb") as f:
            for chunk in r2.iter_content(1024 * 64):
                f.write(chunk)

        log(f"[UPDATE] Downloaded new encoder to {encoder_path}", level="DEBUG")

        current_exe = sys.executable
        log(f"[UPDATE] Launching updater to replace: {current_exe}", level="LOG")
        os.system("title ClosedTelecom Receiver - Updating...")

        subprocess.Popen([
            updater_path,
            "-old", current_exe,
            "-new", encoder_path
        ], shell=True)

        time.sleep(1.0)
        os._exit(0)

    except Exception as e:
        log(f"[-] Update failed: {e}", level="DEBUG")

def delayed_exit():
    time.sleep(1.0)
    os._exit(0)

def on_message(client, userdata, msg, properties=None):
    try:
        if config.get("udp") == True and conn is not None:
            threading.Thread(target=handle_message_udp, args=(client, userdata, msg), daemon=True).start()
        else:
            threading.Thread(target=handle_message_non_udp, args=(client, userdata, msg), daemon=True).start()
    except Exception as e:
        log(f"[-] Error in on_message: {e}", level="DEBUG")


def hourly_resubscribe_loop(client, topics):
    while True:
        try:
            now = datetime.now()
            seconds_until_next_hour = 3600 - (now.minute * 60 + now.second)
            time.sleep(seconds_until_next_hour)

            log("[RECONNECT] Re-subscribing to all topics at top of the hour", level="LOG")
            for topic in topics:
                try:
                    client.subscribe(topic)
                    log(f"[RESUB] Re-subscribed to topic: {topic}", level="DEBUG")
                except Exception as e:
                    log(f"[-] Failed to resubscribe to topic {topic}: {e}", level="DEBUG")
        except Exception as e:
            log(f"[-] Error in hourly resubscribe loop: {e}", level="DEBUG")
            time.sleep(60)  # Wait a minute before retrying

import socket, os, time, struct, json, math
import logging
from datetime import datetime

def sendMessage(files, commands, numSgmts, Pri):
    if conn is None:
        log("[-] Cannot send UDP message: socket not initialized", level="DEBUG")
        return
        
    try:
        if Pri == 0:
            MCAST_PORT = 7787
        elif Pri == 1:
            MCAST_PORT = 7788
        else:
            MCAST_PORT = 7787
            
        if not os.path.exists('./msgId.txt'):
            log("Creating MsgId file", "DEBUG")
            with open('./msgId.txt', 'w') as f:
                f.write('1')
                
        with open('./msgId.txt', "r") as f:
            try:
                msgNum = int(f.read())
            except:
                msgNum = int(1)
                log("Something might have messed up with the message number?", "DEBUG")
                
        with open('./msgId.txt', "w") as f:
            f.write(str(msgNum + 1))

        segnmNum = 0

        for filePath, commandOrig in zip(files, commands):
            command = "<MSG><Exec workRequest=\"" + commandOrig + "\" /></MSG>"
            
            if filePath and os.path.isfile(filePath):
                size = os.path.getsize(filePath)
                packRounded = math.ceil(size / 1405) + 1
                numSegments = numSgmts + 3

                encode1 = bytes(command + 'I2MSG', 'UTF-8')
                encode2 = len(command).to_bytes(4, byteorder='little')
                commandFooter = encode1 + encode2

                with open(filePath, "ab") as f:
                    f.write(commandFooter)
                new_size = os.path.getsize(filePath)

                p1 = struct.pack(">BHHHIIBBBBBBBIBIBBB", 18, 1, 0, 16, msgNum, 0,
                                segnmNum, 0, 0, 8, numSegments, 3, 0, 0, 8,
                                packRounded, 0, 0, 0)
                conn.sendto(p1, (MCAST_GRP, MCAST_PORT))

                with open(filePath, "rb") as f:
                    packet_count = 1
                    j = 0
                    while True:
                        data = f.read(BUF_SIZE)
                        if not data:
                            break

                        packetHeader = struct.pack(">BHHHIIBBB", 18, 1, 0, 1405,
                                                msgNum, packet_count, 0, 0, 0)
                        fec = struct.pack("<IBI", packet_count, 0, new_size)
                        payload = data + bytes(BUF_SIZE - len(data)) if len(
                            data) < BUF_SIZE else data
                        conn.sendto(packetHeader + fec + payload,
                                    (MCAST_GRP, MCAST_PORT))
                        packet_count += 1
                        j += 1

                        if j == 1000:
                            time.sleep(2)
                            j = 0

                segnmNum += 1

            else:
                bx = bytes(command + 'I2MSG', 'utf-8')
                packRounded = math.ceil(len(bx) / 1405) + 1
                numSegments = 4

                p1 = struct.pack(">BHHHIIBBBBBBBIBIBBB", 18, 1, 0, 16, msgNum, 0,
                                segnmNum, 0, 0, 8, numSegments, 3, 0, 0, 8,
                                packRounded, 0, 0, 0)
                conn.sendto(p1, (MCAST_GRP, MCAST_PORT))

                packet_count = 1
                j = 0
                i = 0
                new_size = len(bx)

                for offset in range(0, len(bx), BUF_SIZE):
                    chunk = bx[offset:offset + BUF_SIZE]
                    packetHeader = struct.pack(">BHHHIIBBB", 18, 1, 0, 1405,
                                            msgNum, packet_count, 0, 0, 0)
                    fec = struct.pack("<IBI", packet_count, 0, new_size)
                    payload = chunk + bytes(BUF_SIZE - len(chunk)) if len(
                        chunk) < BUF_SIZE else chunk
                    conn.sendto(packetHeader + fec + payload,
                                (MCAST_GRP, MCAST_PORT))
                    packet_count += 1
                    j += 1
                    if j == 1000:
                        time.sleep(2)
                        j = 0

                segnmNum += 1

        for _ in range(3):
            p3 = struct.pack(">BHHHIIBBBBBBBI", 18, 1, 1, 8, msgNum, 0, segnmNum,
                            0, 0, 8, 0, 0, 0, 67108864)
            p4 = struct.pack(">BHHHIIBBB", 18, 1, 1, 14, msgNum, 1, segnmNum, 0,
                            0) + b"hi"
            conn.sendto(p3, (MCAST_GRP, MCAST_PORT))
            conn.sendto(p4, (MCAST_GRP, MCAST_PORT))
            segnmNum += 1
    except Exception as e:
        log(f"[-] Error in sendMessage: {e}", level="DEBUG")

import os
import json
import base64

def handle_message_udp(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode(errors="ignore")
        log(f"[MSG] Topic: {topic} | {len(payload)} bytes", level="DEBUG")

        try:
            obj = json.loads(payload)
        except json.JSONDecodeError as e:
            log(f"[ERROR] JSON decode failed: {e}", level="DEBUG")
            return

        command = obj.get("workRequest", "")
        file_path = obj.get("fileName", "") or obj.get("filename", "")
        num_segments = obj.get("segments", 0)
        priority = 1

        files = []
        commands = []
        
        if topic == "i2/heartbeat":
            try:
                port = 123
                buf = 1024
                address = ('pool.ntp.org', port)
                msg = b'\x1b' + 47 * b'\0'

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                        s.settimeout(5)
                        s.sendto(msg, address)
                        msg, _ = s.recvfrom(buf)

                    t = struct.unpack("!12I", msg)[10]
                    t -= 2208988800
                    utc_dt = datetime.utcfromtimestamp(t).replace(
                        tzinfo=timezone.utc)
                    formatted_time = utc_dt.strftime("%m/%d/%Y %H:%M:%S") + f".{utc_dt.microsecond // 1000:03d}"
                    commands = ["heartbeat(File={0}," + f"Time={formatted_time})"]
                except Exception as e:
                    log(f"[-] Failed to get NTP time: {e}", level="DEBUG")
                    return
            except Exception as e:
                log(f"[-] Failed to parse heartbeat message: {e}", level="DEBUG")
                return

        if file_path and topic == "i2/radar":
            location = obj.get("location", "").strip()
            image_type = obj.get("imageType", "").strip()
            file_name = file_path or "unnamed_file"
            b64data = obj.get("data")

            full_path = os.path.abspath(os.path.join("./temp/frames", location, image_type, file_name))

            if not b64data:
                log("[-] Missing image data in payload", level="DEBUG")
                return

            if is_in_cache(full_path):
                log(f"[CACHE] File already processed: {full_path}", level="DEBUG")
                return

            try:
                image_data = base64.b64decode(b64data)
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                with open(full_path, "wb") as f:
                    f.write(image_data)
                add_to_cache(full_path)
                files = [full_path]
            except Exception as e:
                log(f"[ERROR] Failed to save image: {e}", level="DEBUG")
                return

            if command:
                formatted_command = command.replace("{filepath}", "{0}")
                commands = [formatted_command]
                
        elif file_path and topic == "i2/data":
            parsed_json = json.loads(payload)
            file_name = parsed_json.get("fileName") or f"message_{datetime.now().strftime('%Y%m%d_%H%M%S')}.i2m"
            xml_data = parsed_json.get("data")
            
            if not xml_data:
                log("[-] No XML data found in payload", level="DEBUG")
                handle_work_request(parsed_json.get("workRequest"))
                return

            filepath = os.path.join("./temp", file_name)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(xml_data)
                commands = [parsed_json.get("workRequest", "").replace("{filepath}", "{0}")]
                files = [filepath]

            log(f"[XML] Saved data to {filepath}", level="DEBUG")

            if command:
                formatted_command = command.replace("{filepath}", "{0}")
                commands = [formatted_command.replace("{filepath}", "{0}")]
        elif file_path:
            formatted_command = command.replace("{0}", os.path.basename(file_path))
            
            commands = [formatted_command.replace("{filepath}", "{0}")]
            if obj.get("payloadType") and obj.get("payloadType") == "i2StarBundle":
                try:
                    obj = json.loads(payload)
                    file_name = obj.get("fileName") or obj.get(
                        "filename") or "unnamed_file"

                    filename = os.path.abspath(
                        os.path.join("./temp/bundles", file_name))

                    b64data = obj.get("data")

                    if not filename or not b64data:
                        log("[-] Invalid image payload", level="DEBUG")
                        return

                    if is_in_cache(filename):
                        return

                    image_data = base64.b64decode(b64data)
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    with open(filename, "wb") as f:
                        f.write(image_data)
                        f.flush()
                        os.fsync(f.fileno())
                    add_to_cache(filename)
                    files = [filename]
                    log(f"[IMG] Saved bundle as {filename}", level="DEBUG")
                except Exception as e:
                    log(f"[-] Failed to parse bundle message: {e}", level="DEBUG")
                    return
            else:
                files = [file_path]
        else:
            files = [""]
            commands = [command.replace("{filepath}", "{0}")]
            
        if files == ['']:
            os.makedirs(os.path.join("./temp/"), exist_ok=True)
            with open(os.path.join("./temp/temp.i2m"), "w", encoding="utf-8") as f:
                f.write("This is a test")
            files = [os.path.join("./temp/temp.i2m")]
            
        log(f"[SEND] Files: {files}, Commands: {commands}", level="DEBUG")
        sendMessage(files, commands, num_segments, 0)
    except Exception as e:
        log(f"[-] Error in handle_message_udp: {e}", level="DEBUG")
        traceback.print_exc()


def handle_message_non_udp(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode(errors="ignore")
        log(f"[MSG] Topic: {topic} | {len(payload)} bytes", level="DEBUG")

        try:
            obj = json.loads(payload)
        except json.JSONDecodeError as e:
            log(f"[ERROR] JSON decode failed: {e}", level="DEBUG")
            return

        if obj.get("payloadType") and obj.get("payloadType") == "i2StarBundle":
            try:
                obj = json.loads(payload)
                file_name = obj.get("fileName") or obj.get(
                    "filename") or "unnamed_file"

                filename = os.path.abspath(
                    os.path.join("./temp/bundles", file_name))

                b64data = obj.get("data")

                if not filename or not b64data:
                    log("[-] Invalid image payload", level="DEBUG")
                    return

                if is_in_cache(filename):
                    return

                image_data = base64.b64decode(b64data)
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, "wb") as f:
                    f.write(image_data)
                    f.flush()
                    os.fsync(f.fileno())
                workReq = obj.get("workRequest",
                                  "").replace("{filepath}",
                                              os.path.abspath(filename))
                handle_work_request(workReq)
                add_to_cache(filename)

                log(f"[IMG] Saved bundle as {filename}", level="DEBUG")
                return
            except Exception as e:
                log(f"[-] Failed to parse bundle message: {e}", level="DEBUG")
                return

        if topic == "i2/heartbeat":
            try:
                port = 123
                buf = 1024
                address = ('pool.ntp.org', port)
                msg = b'\x1b' + 47 * b'\0'

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                        s.settimeout(5)
                        s.sendto(msg, address)
                        msg, _ = s.recvfrom(buf)

                    t = struct.unpack("!12I", msg)[10]
                    t -= 2208988800
                    utc_dt = datetime.utcfromtimestamp(t).replace(
                        tzinfo=timezone.utc)
                    formatted_time = utc_dt.strftime("%m/%d/%Y %H:%M:%S") + f".{utc_dt.microsecond // 1000:03d}"
                    handle_work_request(f"heartbeat(Time={formatted_time})")
                except Exception as e:
                    log(f"[-] Failed to get NTP time: {e}", level="DEBUG")
                    return
            except Exception as e:
                log(f"[-] Failed to parse heartbeat message: {e}", level="DEBUG")
                return

        if topic == IMAGE_TOPIC:
            try:
                obj = json.loads(payload)
                location = obj.get("location", "").strip()
                image_type = obj.get("imageType", "").strip()
                file_name = obj.get("fileName") or obj.get(
                    "filename") or "unnamed_file"

                filename = os.path.abspath(
                    os.path.join("./temp/frames", location, image_type, file_name))

                b64data = obj.get("data")

                if not filename or not b64data:
                    log("[-] Invalid image payload", level="DEBUG")
                    return

                if is_in_cache(filename):
                    return

                image_data = base64.b64decode(b64data)
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, "wb") as f:
                    f.write(image_data)
                    f.flush()
                    os.fsync(f.fileno())
                workReq = obj.get("workRequest",
                                  "").replace("{filepath}",
                                              os.path.abspath(filename))
                handle_work_request(workReq)
                add_to_cache(filename)
                log(f"[IMG] Saved image as {filename}", level="DEBUG")
                return
            except Exception as e:
                log(f"[-] Failed to parse image message: {e}", level="DEBUG")
                return

        try:
            parsed_json = json.loads(payload)
            os.makedirs("./temp", exist_ok=True)

            filename = parsed_json.get(
                "fileName"
            ) or f"message_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
            xml_data = parsed_json.get("data")

            if not xml_data:
                log("[-] No XML data found in payload", level="DEBUG")
                handle_work_request(parsed_json.get("workRequest"))
                return

            filepath = os.path.join("./temp", filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(xml_data)
                f.flush()
                os.fsync(f.fileno())
            workReq = parsed_json.get("workRequest",
                                      "").replace("{filepath}",
                                                  os.path.abspath(filepath))
            handle_work_request(workReq)

            log(f"[XML] Saved data to {filepath}", level="DEBUG")

        except Exception as e:
            log(f"[-] Failed to save text message: {e}", level="DEBUG")
    except Exception as e:
        log(f"[-] Error in handle_message_non_udp: {e}", level="DEBUG")
        traceback.print_exc()

def main():
    try:
        temp_dir = "./temp"
        frames_dir = os.path.join(temp_dir, "frames")

        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                log(f"[CLEANUP] Removed existing {temp_dir} directory",
                    level="DEBUG")
            except Exception as e:
                log(f"[-] Failed to remove {temp_dir}: {e}", level="DEBUG")

        try:
            os.makedirs(frames_dir, exist_ok=True)
            log(f"[SETUP] Created {frames_dir} directory", level="DEBUG")
        except Exception as e:
            log(f"[-] Failed to create {frames_dir}: {e}", level="DEBUG")

        load_cache()

        print(
            r"""                                                                      
 $$$$$$\  $$\                                     $$\ $$$$$$$$\        $$\                                             
$$  __$$\ $$ |                                    $$ |\__$$  __|       $$ |                                            
$$ /  \__|$$ | $$$$$$\   $$$$$$$\  $$$$$$\   $$$$$$$ |   $$ | $$$$$$\  $$ | $$$$$$\   $$$$$$$\  $$$$$$\  $$$$$$\$$$$\  
$$ |      $$ |$$  __$$\ $$  _____|$$  __$$\ $$  __$$ |   $$ |$$  __$$\ $$ |$$  __$$\ $$  _____|$$  __$$\ $$  _$$  _$$\ 
$$ |      $$ |$$ /  $$ |\$$$$$$\  $$$$$$$$ |$$ /  $$ |   $$ |$$$$$$$$ |$$ |$$$$$$$$ |$$ /      $$ /  $$ |$$ / $$ / $$ |
$$ |  $$\ $$ |$$ |  $$ | \____$$\ $$   ____|$$ |  $$ |   $$ |$$   ____|$$ |$$   ____|$$ |      $$ |  $$ |$$ | $$ | $$ |
\$$$$$$  |$$ |\$$$$$$  |$$$$$$$  |\$$$$$$$\ \$$$$$$$ |   $$ |\$$$$$$$\ $$ |\$$$$$$$\ \$$$$$$$\ \$$$$$$  |$$ | $$ | $$ |
 \______/ \__| \______/ \_______/  \_______| \_______|   \__| \_______|\__| \_______| \_______| \______/ \__| \__| \__|
$$$$$$$\                                $$\                                           $$$$$$\                          
$$  __$$\                               \__|                                         $$ ___$$\                         
$$ |  $$ | $$$$$$\   $$$$$$$\  $$$$$$\  $$\ $$\    $$\  $$$$$$\   $$$$$$\        $$\ \_/   $$ |                        
$$$$$$$  |$$  __$$\ $$  _____|$$  __$$\ $$ |\$$\  $$  |$$  __$$\ $$  __$$\       \__|  $$$$$ /                         
$$  __$$< $$$$$$$$ |$$ /      $$$$$$$$ |$$ | \$$\$$  / $$$$$$$$ |$$ |  \__|            \___$$\                         
$$ |  $$ |$$   ____|$$ |      $$   ____|$$ |  \$$$  /  $$   ____|$$ |            $$\ $$\   $$ |                        
$$ |  $$ |\$$$$$$$\ \$$$$$$$\ \$$$$$$$\ $$ |   \$  /   \$$$$$$$\ $$ |            \__|\$$$$$$  |                        
\__|  \__| \_______| \_______| \_______|\__|    \_/     \_______|\__|                 \______/                        
""")
        print("ClosedTelecom Receiver :3")
        print("Decompiled by ClosedTelecom :3 :3 :3 :3 :3 :3 :3 :3 :3 :3 :3 :3 :3 :3.")
        print("Version 1.1.1 built on 08-12-2036.")
        print("\n")

        os.system(
            "title ClosedTelecom Receiver - v1.1.1 (checking for updates...)")
        
        global API_SERVER, apiKey
        API_SERVER = str(config["server"]) + ":" + str(config["port"])
        if config.get("tls") == True:
            API_SERVER = "https://" + API_SERVER
        else:
            API_SERVER = "http://" + API_SERVER
            
        apiKey = config["apiKey"]
        
        if check_for_updates(API_SERVER, APP_VERSION, config["apiKey"]):
            log("yo, your receiver is out of date!", level="LOG")
            #log("[UPDATE] Downloading new version...", level="LOG")
            #download_and_launch_updater(API_SERVER, config["apiKey"])
            #sys.exit(0)
            
        thread = threading.Thread(target=update_check_loop,
                                  args=(API_SERVER, config["apiKey"]),
                                  daemon=True)
        thread.start()

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             transport="websockets",
                             userdata=config)
        client.username_pw_set(config["apiKey"])

        if len(sys.argv) >= 3 and sys.argv[1] == "-s" and sys.argv[2] == "update":
            log("Update complete. Thank you for using ClosedTelecom Receiver!", level="LOG")

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        
        if config.get("tls") == True:
            client.tls_set()
            
        client.connect(config["server"], config["port"])
        os.system("title ClosedTelecom Receiver - v1.1.1 (connected)")
        client.loop_forever()
        
    except KeyboardInterrupt:
        log("\n[EXIT] Shutting down gracefully...", level="LOG")
        sys.exit(0)
    except Exception as e:
        log(f"[-] Fatal error in main: {e}", level="LOG")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()