from neo_fairy_client import FairyClient, Hash160Str
from enum import Enum

user = Hash160Str('0xb1983fa2479a0c8e2beae032d2df564b5451b7a5')
c = FairyClient(fairy_session='RelationalDB', wallet_address_or_scripthash=user, with_print=False)
c.virutal_deploy_from_path('./bin/sc/RelationalDB.nef')
print(result := c.invokefunction('encodeInteger', [12345678]))
assert c.invokefunction('decodeInteger', [result]) == (12345678, '')
print(result := c.invokefunction('encodeInteger', [-12345678]))
assert c.invokefunction('decodeInteger', [result]) == (-12345678, '')


class Types(bytes, Enum):
    Boolean = b'\x20'
    IntegerVarLen = b'\x21'
    ByteStringVarLen = b'\x28'
    IntegerFixedLen = b'\x31'
    ByteStringFixedLen = b'\x38'
    
    def __add__(self, other):
        return self.value + other.value


table_name = 'testTable'
assert 'Invalid type' in c.invokefunction('createTable', [user, table_name, Types.Boolean + Types.IntegerVarLen + b'\x00'], do_not_raise_on_result=True)
c.invokefunction('createTable', [user, 'testTable', Types.Boolean + Types.IntegerVarLen + Types.ByteStringVarLen + Types.IntegerFixedLen + b'\x04' + Types.ByteStringFixedLen + b'\x20'])
assert len(c.invokefunction('listTables', [user, 'test'])) == 1
assert len(c.invokefunction('listAllTables')) == 1
assert len(c.invokefunction('listDroppedTables', [user, ''])) == 0
assert len(c.invokefunction('listAllDroppedTables')) == 0
c.invokefunction('dropTable', [user, table_name])
assert len(c.invokefunction('listTables', [user, ''])) == 0
assert len(c.invokefunction('listAllTables')) == 0
assert len(c.invokefunction('listDroppedTables', [user, ''])) == 1
assert len(c.invokefunction('listAllDroppedTables')) == 1
