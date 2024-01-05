from neo_fairy_client import FairyClient

c = FairyClient(fairy_session='RelationalDB')
c.virutal_deploy_from_path('./bin/sc/RelationalDB.nef')
print(result := c.invokefunction('encodeInteger', [12345678]))
assert c.invokefunction('decodeInteger', [result]) == (12345678, '')
print(result := c.invokefunction('encodeInteger', [-12345678]))
assert c.invokefunction('decodeInteger', [result]) == (-12345678, '')
