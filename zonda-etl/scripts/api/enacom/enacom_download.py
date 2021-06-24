import requests
import ssl

def create_ssl_context(proto=ssl.PROTOCOL_SSLv23,
                       verify_mode=ssl.CERT_NONE,
                       protocols=None,
                       options=None,
                       ciphers="ALL"):
    protocols = protocols or ('PROTOCOL_SSLv3', 'PROTOCOL_TLSv1',
                              'PROTOCOL_TLSv1_1', 'PROTOCOL_TLSv1_2')
    options = options or ('OP_CIPHER_SERVER_PREFERENCE', 'OP_SINGLE_DH_USE',
                          'OP_SINGLE_ECDH_USE', 'OP_NO_COMPRESSION')
    context = ssl.SSLContext(proto)
    context.verify_mode = verify_mode
    # reset protocol, options
    context.protocol = 0
    context.options = 0
    for p in protocols:
        context.protocol |= getattr(ssl, p, 0)
    for o in options:
        context.options |= getattr(ssl, o, 0)
    context.set_ciphers(ciphers)
    return context

proxies = {
    "http": "http://proxy.ar.bsch:8080",
    "https": "https://proxy.ar.bsch:8080"
}
site = requests.get('https://www.enacom.gob.ar', proxies=proxies, verify=False)
print(site)