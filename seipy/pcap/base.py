import scapy


# import scapy_ssl_tls as scl # python 2


def read_pcap(pcap_file):
    """
    light wrapper over scapy's pcap reader.
    """
    return scapy.utils.rdpcap(pcap_file)
