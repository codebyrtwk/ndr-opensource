import socket
import struct
from datetime import datetime
import scapy.all as scapy
import subprocess
import re 
# from analyzer.pca_analysis import pca_analysis

def parse_dns(data):
    """Parse DNS packet."""
    transaction_id, flags, qd_count, an_count, ns_count, ar_count = struct.unpack('! H H H H H H', data[:12])
    query = data[12:]
    domain_name = ''
    i = 0
    while True:
        length = query[i]
        if length == 0:
            break
        domain_name += query[i+1:i+1+length].decode() + '.'
        i += length + 1
    
    dns_info = {
        "transaction_id": transaction_id,
        "flags": flags,
        "qd_count": qd_count,
        "an_count": an_count,
        "ns_count": ns_count,
        "ar_count": ar_count,
        "domain_name": domain_name
    }
    return dns_info

def ethernet_frame_parser(raw_data):
    # Convert the hex string to bytes
    packet_data = bytes.fromhex(raw_data)
    
    # Unpack the Ethernet frame header
    dest_mac, src_mac, proto = struct.unpack('! 6s 6s H', packet_data[:14])
    
    # Extract the MAC addresses and protocol
    dest_mac_addr = ':'.join(f'{b:02x}' for b in dest_mac)
    src_mac_addr = ':'.join(f'{b:02x}' for b in src_mac)
    eth_proto = socket.ntohs(proto)
    
    # Return the parsed values
    return dest_mac_addr, src_mac_addr, eth_proto, packet_data[14:]
def unpack_ipv4_packet(data):
    version_header_length = data[0]
    version = version_header_length >> 4
    header_length = (version_header_length & 15) * 4
    ttl, proto, src, target = struct.unpack('! 8x B B 2x 4s 4s', data[:20])
    return version, header_length, ttl, proto, format_ip(src), format_ip(target), data[header_length:]

def format_ip(addr):
    return '.'.join(map(str, addr))

def format_mac_addr(raw_mac_addr):
    raw_str = map('{:02x}'.format, raw_mac_addr)
    return ':'.join(raw_str).upper()

def unpack_proto(proto, data):
    if proto == 1:
        protocol_name = "ICMP"
        proto_type, code, checksum = struct.unpack('! B B H', data[:4])
        protocol_data = {
            "protocol_name": protocol_name, "proto_type": proto_type, "code": code, "checksum": checksum, "data": data[4:] 
        }
        
    elif proto == 6:
        protocol_name = "TCP"
        src_port, dest_port, sequence, acknowledgement, offset_reserved_flag = struct.unpack("! H H L L H", data[:14])
        offset = (offset_reserved_flag >> 12) * 4
        protocol_data = {
            "protocol_name": protocol_name, "src_port": src_port, "dest_port": dest_port, "sequence": sequence, "acknowledgement": acknowledgement,
            "flag_urg": (offset_reserved_flag & 32) >> 5, "flag_ack": (offset_reserved_flag & 16) >> 4, "flag_psh": (offset_reserved_flag & 8) >> 3,
            "flag_rst": (offset_reserved_flag & 4) >> 2, "flag_syn": (offset_reserved_flag & 2) >> 1, "flag_fin": offset_reserved_flag & 1, "data": data[offset:]
        }
        
    elif proto == 17:
        src_port, dest_port, size = struct.unpack('! H H 2x H', data[:8])
        if src_port == 53 or dest_port == 53:
            dns_info = parse_dns(data[8:])
            protocol_data = {"protocol_name": "DNS", "dns_info": dns_info}
        else:
            protocol_data = {"protocol_name": "UDP", "src_port": src_port, "dest_port": dest_port, "size": size, "data": data[8:]}
        
    return protocol_data

def scan_network(ip_range):
    """ Scan the network for devices using nmap """
    result = subprocess.run(['nmap', '-sn', ip_range], capture_output=True, text=True)
    return result.stdout

def arp_scan(ip_range):
    """ Scan the network using ARP """
    arp_request = scapy.ARP(pdst=ip_range)
    broadcast = scapy.Ether(dst="ff:ff:ff:ff:ff:ff")
    arp_request_broadcast = broadcast/arp_request
    answered_list = scapy.srp(arp_request_broadcast, timeout=1, verbose=False)[0]
    
    devices_list = []
    for element in answered_list:
        device_info = {"ip": element[1].psrc, "mac": element[1].hwsrc}
        devices_list.append(device_info)
    
    return devices_list

def print_devices(devices):
    """ Print list of devices """
    print("IP Address\t\tMAC Address")
    print("-----------------------------------------")
    for device in devices:
        print(f"{device['ip']}\t\t{device['mac']}")
        
def get_device_name(ip):
    try:
        return socket.gethostbyaddr(ip)[0]
    except socket.herror as e:
        print(e)
        return None
        
def parse_event(event):
    '''V1: Basic Function to parse data packet logs , Considering raw logs coming from ethernet --> Features -- 
        Source IP , Dest IP , Source Port , Dest Port , Protocol , Payload '''
    start_ts = datetime.now()
    if event.get("source_type") == "packet":
        parsed_packet = {}
        dest_mac_addr, src_mac_addr, eth_proto, payload = ethernet_frame_parser(event["raw_data"])
        print(eth_proto)
        if eth_proto == 8:  # IPv4
            version, header_length, ttl, proto, src_ip, dest_ip, data = unpack_ipv4_packet(payload)
            print(version , header_length , ttl , proto )
            proto_data = unpack_proto(proto, data)
            parsed_packet["tenant"] = event.get("tenant")
            parsed_packet["source"] = event.get("source")
            parsed_packet["src_device_name"] = get_device_name(src_ip)
            parsed_packet["dest_device_name"] = get_device_name(dest_ip)
            parsed_packet["src_device_name"] = get_device_name(src_ip)
            parsed_packet["dest_device_name"] = get_device_name(dest_ip)
            parsed_packet["timestamp"] = datetime.now().isoformat()
            parsed_packet["src_mac_addr"] = src_mac_addr
            parsed_packet["dest_mac_addr"] = dest_mac_addr
            parsed_packet["src_ip_addr"] = src_ip
            parsed_packet["dest_ip_addr"] = dest_ip
            parsed_packet["proto"] = proto
            parsed_packet["ttl"] = ttl
            parsed_packet["version"] = version
            parsed_packet["header_length"] = header_length
            parsed_packet.update(proto_data)  # Add protocol-specific data
            end_ts = datetime.now()
            latency = end_ts-start_ts
            parsed_packet["event_parse_latency_ms"] = int(latency.total_seconds() * 1000) 
            parsed_packet["is_parsed"] = True
            # packets.append(parsed_packet)  # Store packet information
            # if len(packets >= 5):
            #     pca_analysis_reuslt = pca_analysis.pca_analysis({"Record" : packets})
            return parsed_packet
    else:
        pass