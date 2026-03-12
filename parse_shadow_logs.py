import os
import re
import json
import glob
from datetime import datetime

# ANSI escape sequence regex
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*[mK]')

def strip_ansi(text):
    return ANSI_ESCAPE.sub('', text)

# Common city codes found in the GML (geoname_ids) to lat/lon
CITY_COORD_MAPPING = {
    "3177664": (45.55, 9.18),   # Cusano Milanino, IT
    "2660939": (47.37, 8.54),   # Zurich, CH
    "1862258": (35.69, 139.69), # Tokyo, JP
    "2984881": (48.01, 0.20),   # Le Mans, FR
    "3062888": (50.08, 14.43),  # Prague, CZ
    "2513416": (36.72, -4.42),  # Malaga, ES
    "5713376": (43.61, -116.20),# Boise, US
    "2867714": (51.22, 6.77),   # Dusseldorf, DE
    "5240509": (41.49, -81.69), # Cleveland, US
    "2878313": (50.94, 6.96),   # Cologne, DE
    "2809346": (51.05, 3.73),   # Ghent, BE
    "3107783": (42.22, 13.56),  # L'Aquila, IT (approx)
    "1275004": (19.07, 72.87),  # Mumbai, IN
    "2796637": (51.05, 4.48),   # Mechelen, BE
    "5105496": (40.22, -74.75), # Trenton, US
    "2514169": (37.38, -5.98),  # Seville, ES
    "2640692": (51.52, -3.18),  # Cardiff, GB
    "3077882": (49.44, 15.59),  # Jihlava, CZ
    "3088171": (51.10, 17.03),  # Wroclaw, PL
    "5128581": (40.71, -74.00), # New York, US
    "5368361": (34.05, -118.24),# Los Angeles, US
    "4887398": (41.87, -87.62), # Chicago, US
    "2643743": (51.50, -0.12),  # London, GB
    "2950159": (52.52, 13.40),  # Berlin, DE
    "2968815": (48.85, 2.35),   # Paris, FR
    "3128760": (40.41, -3.70),  # Madrid, ES
    "6455259": (52.36, 4.90),   # Amsterdam, NL
    "3117735": (40.41, -3.70),  # Madrid, ES
    "1850147": (35.68, 139.65), # Tokyo, JP
    "1819729": (22.39, 114.10), # Hong Kong, HK
}

# Country fallback coordinates (center of country)
COUNTRY_COORD_MAPPING = {
    "IT": (41.87, 12.56),
    "CH": (46.81, 8.22),
    "JP": (36.20, 138.25),
    "FR": (46.22, 2.21),
    "CZ": (49.81, 15.47),
    "ES": (40.46, -3.74),
    "US": (37.09, -95.71),
    "DE": (51.16, 10.45),
    "BE": (50.50, 4.46),
    "IN": (20.59, 78.96),
    "GB": (55.37, -3.43),
    "PL": (51.91, 19.14),
    "NL": (52.13, 5.29),
    "HK": (22.31, 114.16),
    "CA": (56.13, -106.34),
    "AU": (-25.27, 133.77),
    "BR": (-14.23, -51.92),
    "RU": (61.52, 105.31),
    "CN": (35.86, 104.19),
    "KR": (35.90, 127.76),
    "SG": (1.35, 103.81),
}

# Role to Color mapping
ROLE_COLORS = {
    "Builder": [147, 112, 219],   # Purple
    "Proposer": [255, 69, 0],     # Red-Orange
    "PTC": [255, 215, 0],         # Gold
    "Sampler": [30, 144, 255],    # Blue
    "Provider": [0, 191, 255],    # Light Blue
    "Default": [200, 200, 200]
}

# Topic to Color mapping
TOPIC_COLORS = {
    "/cl/ptc_attestation": [0, 255, 255],      # Cyan
    "/cl/payload_envelope": [255, 0, 255],     # Magenta
    "/cl/blob_sidecar": [0, 255, 0],           # Green
    "/cl/beacon_block": [255, 255, 255],       # White
    "/cl/bids": [255, 165, 0],                 # Orange
    "/el/blob_hash": [128, 128, 128],          # Gray
    "Default": [100, 100, 100]
}

def parse_gml(gml_path):
    print(f"Parsing GML: {gml_path}")
    nodes = {}
    current_node = None
    with open(gml_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith("node ["):
                current_node = {}
            elif line.startswith("id ") and current_node is not None:
                current_node['id'] = int(line.split()[1])
            elif line.startswith("city_code ") and current_node is not None:
                current_node['city_code'] = line.split('"')[1]
            elif line.startswith("country_code ") and current_node is not None:
                current_node['country_code'] = line.split('"')[1]
            elif line == "]":
                if current_node and 'id' in current_node:
                    nodes[current_node['id']] = current_node
                current_node = None
    return nodes

def parse_shadow_config(config_path):
    print(f"Parsing Shadow Config: {config_path}")
    host_to_node = {}
    current_host = None
    with open(config_path, 'r') as f:
        for line in f:
            # Match "  hostname:" at the start of the hosts section
            host_match = re.match(r"^  ([^ :]+):", line)
            if host_match:
                current_host = host_match.group(1)
            # Match "    network_node_id: ID"
            node_match = re.match(r"^    network_node_id: (\d+)", line)
            if node_match and current_host:
                host_to_node[current_host] = int(node_match.group(1))
    return host_to_node

def parse_logs(data_dir, host_to_node, nodes_geo):
    print(f"Parsing logs in {data_dir}")
    nodes_data = {}
    traffic_data = []
    
    # Message ID correlation map: msg_id -> (source_peer_id, start_time_ms, topic)
    msg_sources = {}

    # First pass: find peer IDs and roles
    for host_dir in sorted(glob.glob(os.path.join(data_dir, 'hosts', '*'))):
        hostname = os.path.basename(host_dir)
        node_id = host_to_node.get(hostname)
        if node_id is None:
            continue

        stdout_path = os.path.join(host_dir, 'blob-sim.1000.stdout')
        if not os.path.exists(stdout_path):
            continue

        local_peer_id = None
        role = "Default"
        
        with open(stdout_path, 'r') as f:
            for line in f:
                line = strip_ansi(line)
                # Find local peer ID
                peer_match = re.search(r"local_peer_id=([a-zA-Z0-9]+)", line)
                if peer_match:
                    local_peer_id = peer_match.group(1)
                
                # Find roles
                role_match = re.search(r"starting slot ticker.*roles=([a-z+]+)", line)
                if role_match:
                    roles_str = role_match.group(1)
                    if "builder" in roles_str:
                        role = "Builder"
                    elif "proposer" in roles_str:
                        role = "Proposer"
                    elif "ptc" in roles_str:
                        role = "PTC"
                    elif "sampler" in roles_str:
                        role = "Sampler"
                    elif "provider" in roles_str:
                        role = "Provider"
        
        if not local_peer_id:
            continue

        # Map node to geo
        geo = nodes_geo.get(node_id, {})
        city_code = geo.get('city_code')
        country_code = geo.get('country_code')
        
        lat, lon = (0, 0)
        if city_code in CITY_COORD_MAPPING:
            lat, lon = CITY_COORD_MAPPING[city_code]
        elif country_code in COUNTRY_COORD_MAPPING:
            lat, lon = COUNTRY_COORD_MAPPING[country_code]
        
        # Jitter based on hostname hash to avoid exact overlap
        h_hash = hash(hostname)
        jitter_lat = ((h_hash % 1000) / 500.0) - 1.0 # +/- 1.0 degree
        jitter_lon = (((h_hash // 1000) % 1000) / 500.0) - 1.0

        if (lat, lon) == (0, 0):
            # Global fallback jitter if no city/country match
            lat = (node_id % 160) - 80
            lon = (node_id * 7 % 340) - 170
        else:
            lat += jitter_lat
            lon += jitter_lon

        nodes_data[local_peer_id] = {
            "id": local_peer_id,
            "node_id": node_id,
            "lat": lat,
            "lon": lon,
            "role": role,
            "color": ROLE_COLORS.get(role, ROLE_COLORS["Default"])
        }

    # Second pass: traffic correlation
    # msg_id -> [(peer_id, arrival_time_ms)]
    msg_receptions = {}
    # msg_id -> [(source_peer_id, forward_time_ms)]
    msg_forwards = {}

    for host_dir in glob.glob(os.path.join(data_dir, 'hosts', '*')):
        hostname = os.path.basename(host_dir)
        stdout_path = os.path.join(host_dir, 'blob-sim.1000.stdout')
        if not os.path.exists(stdout_path):
            continue

        local_peer_id = None
        with open(stdout_path, 'r') as f:
            for line in f:
                line = strip_ansi(line)
                if not local_peer_id:
                    peer_match = re.search(r"local_peer_id=([a-zA-Z0-9]+)", line)
                    if peer_match: local_peer_id = peer_match.group(1)
                    continue

                time_match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)Z", line)
                if not time_match: continue
                
                ts_str = time_match.group(1)
                dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f")
                ts_ms = (dt.minute * 60 + dt.second) * 1000 + dt.microsecond // 1000
                
                # Check for publication
                if "gossip message published" in line:
                    topic_match = re.search(r"topic=([^ ]+)", line)
                    msg_id_match = re.search(r"msg_id=([^ ]+)", line)
                    if topic_match and msg_id_match:
                        topic = topic_match.group(1)
                        msg_id = msg_id_match.group(1)
                        msg_sources[msg_id] = (local_peer_id, ts_ms, topic)

                # Check for reception
                if "gossip message received" in line:
                    source_match = re.search(r"propagation_source=([^ ]+)", line)
                    msg_id_match = re.search(r"message_id=([^ ]+)", line)
                    topic_match = re.search(r"topic=([^ ]+)", line)
                    
                    if source_match and msg_id_match and topic_match:
                        source_peer = source_match.group(1)
                        msg_id = msg_id_match.group(1)
                        topic = topic_match.group(1)
                        
                        if msg_id not in msg_receptions: msg_receptions[msg_id] = []
                        msg_receptions[msg_id].append((local_peer_id, ts_ms, source_peer, topic))

                # Check for forwarding (new log)
                if "gossip message forwarded" in line:
                    msg_id_match = re.search(r"message_id=([^ ]+)", line)
                    if msg_id_match:
                        msg_id = msg_id_match.group(1)
                        if msg_id not in msg_forwards: msg_forwards[msg_id] = []
                        msg_forwards[msg_id].append((local_peer_id, ts_ms))

    # Now reconcile all events into traffic.json
    for msg_id, receptions in msg_receptions.items():
        for target_peer, arrival_time, source_peer, topic in receptions:
            # The log tells us exactly who the propagation_source was for this reception
            # We just need to find the timestamp when the source sent/forwarded it
            
            start_time = arrival_time - 50 # Default
            
            # Case 1: Was it the original publisher?
            if msg_id in msg_sources:
                orig_pub, pub_time, _ = msg_sources[msg_id]
                if source_peer == orig_pub:
                    start_time = pub_time
            
            # Case 2: Was it forwarded by someone?
            if msg_id in msg_forwards:
                for fwd_peer, fwd_time in msg_forwards[msg_id]:
                    if fwd_peer == source_peer:
                        start_time = fwd_time
                        break

            base_topic = '/'.join(topic.split('/')[:3])
            color = TOPIC_COLORS.get(base_topic, TOPIC_COLORS["Default"])
            
            traffic_data.append({
                "source_id": source_peer,
                "target_id": target_peer,
                "start_time_ms": start_time,
                "arrival_time_ms": arrival_time,
                "topic": topic,
                "color": color
            })

    return list(nodes_data.values()), traffic_data

def main():
    gml_path = "atlas_v201801.shadow_v2.gml"
    config_path = "shadow.data/processed-config.yaml"
    data_dir = "shadow.data"
    
    nodes_geo = parse_gml(gml_path)
    host_to_node = parse_shadow_config(config_path)
    nodes_json, traffic_json = parse_logs(data_dir, host_to_node, nodes_geo)
    
    print(f"Writing {len(nodes_json)} nodes to nodes.json")
    with open('nodes.json', 'w') as f:
        json.dump(nodes_json, f, indent=2)
        
    print(f"Writing {len(traffic_json)} traffic events to traffic.json")
    with open('traffic.json', 'w') as f:
        json.dump(traffic_json, f, indent=2)

if __name__ == "__main__":
    main()
