from neo_fairy_client import FairyClient, Hash160Str, Hash256Str
from enum import Enum

user = Hash160Str('0xb1983fa2479a0c8e2beae032d2df564b5451b7a5')
main_session = 'RelationalDB'
c = FairyClient(fairy_session=main_session, wallet_address_or_scripthash=user, with_print=False)
c.virutal_deploy_from_path('./bin/sc/RelationalDB.nef')
assert (result := c.invokefunction('encodeInteger', [12345678])) == b'\x04\x00Na\xbc\x00'
assert c.invokefunction('decodeInteger', [result]) == (12345678, '')
assert (result := c.invokefunction('encodeInteger', [-12345678])) == b'\x04\x00\xb2\x9eC\xff'
assert c.invokefunction('decodeInteger', [result]) == (-12345678, '')
assert len(c.invokefunction('encodeInteger', [-1])) == 3
assert len(c.invokefunction('encodeInteger', [-2**33])) == 7
assert len(result := c.invokefunction('encodeInteger', ['\x00'*32+'\x01'])) == 35
assert 'MaxSize exceed: 33' in c.invokefunction('decodeInteger', [result], do_not_raise_on_result=True)
assert len(c.invokefunction('encodeByteString', ['\x00'*0x0100])) == 0x0102
assert len(c.invokefunction('encodeByteString', ['\x00'*0x8000])) == 0x8002
assert c.invokefunction('encodeIntegerFixedLength', [0x017b, 4]).encode() == b'\x7b\x01\x00\x00'
assert c.invokefunction('encodeIntegerFixedLength', [-1, 4]) == b'\xff\xff\xff\xff'
assert c.invokefunction('decodeIntegerFixedLength', [c.invokefunction('encodeIntegerFixedLength', [-1, 4]), 4]) == (-1, '')
assert 'Too long value' in c.invokefunction('encodeIntegerFixedLength', [2**31, 4], do_not_raise_on_result=True)
assert 'Too long value' in c.invokefunction('encodeIntegerFixedLength', [-2**31-1, 4], do_not_raise_on_result=True)
assert c.invokefunction('decodeIntegerFixedLength', [c.invokefunction('encodeIntegerFixedLength', [2**31-1, 4]), 4]) == (2**31-1, '')
assert c.invokefunction('decodeIntegerFixedLength', [c.invokefunction('encodeIntegerFixedLength', [-2**31, 4]), 4]) == (-2**31, '')


class Types(bytes, Enum):
    UINT160 = b'\x10'
    UINT256 = b'\x11'
    Boolean = b'\x20'
    IntVarLen = b'\x21'
    ByteStringVarLen = b'\x28'
    IntFixedLen = b'\x31'
    ByteStringFixedLen = b'\x38'
    
    def __add__(self, other):
        if type(other) == type(self):
            return self.value + other.value
        return self.value + other


table_name = 'testTable'
assert 'Invalid type' in c.invokefunction('createTable', [user, table_name, Types.Boolean + Types.IntVarLen + b'\x00', True], do_not_raise_on_result=True)
assert 'SEPARATOR in tableName' in c.invokefunction('createTable', [user, 'testTable\x00', Types.Boolean + Types.IntVarLen + Types.ByteStringVarLen + Types.IntFixedLen + b'\x04' + Types.ByteStringFixedLen + b'\x20', True], do_not_raise_on_result=True)
assert c.invokefunction(
    'createTable', [
        user, 'testTable',
        column_types := Types.Boolean + Types.IntVarLen + Types.ByteStringVarLen + Types.IntFixedLen + b'\x04' + Types.ByteStringFixedLen + b'\x20',
        # bool, int, str, int32, str(20)
        True
    ]) == 5
assert c.invokefunction('getColumnType', [user, table_name, 1]).encode() == Types.Boolean
assert c.invokefunction('getColumnType', [user, table_name, 4]).encode() == Types.IntFixedLen + b'\x04'
c.invokefunction('setColumnNames', [user, table_name, column_names := ['bool', 'int', 'bytestring', 'int32', 'bytestring160']])
assert c.invokefunction('getColumnTypeByName', [user, table_name, 'bool']).encode() == Types.Boolean
assert c.invokefunction('getColumnTypeByName', [user, table_name, 'int32']).encode() == Types.IntFixedLen + b'\x04'
assert len(c.invokefunction('findColumnNames', [user, table_name, ''])) == 5
assert len(c.invokefunction('findColumnNames', [user, table_name, 'b'])) == 3
assert len(c.invokefunction('findColumnNames', [user, table_name, 'i'])) == 2
assert len(c.invokefunction('listTables', [user, 'test'])) == 1
assert len(c.invokefunction('listAllTables')) == 1
assert len(c.invokefunction('listDroppedTables', [user, ''])) == 0
assert len(c.invokefunction('listAllDroppedTables')) == 0

assert 'Too long value' in c.invokefunction('writeRow', [user, table_name, [1, 23333, 'test str', 2**31, '0123456789']], do_not_raise_on_result=True)
assert 'Too long value' in c.invokefunction('writeRow', [user, table_name, [1, 23333, 'test str', 2**31-1, '0123456789abcdef 0123456789abcdef']], do_not_raise_on_result=True)
c.invokefunction('writeRow', [user, table_name, [0, 23333, 'test str', 2**31-1, '0123456789abcdef0123456789abcdef']])
c.invokefunction('writeRow', [user, table_name, [1, 233, 'test str 233', 2**31-2, '12345678901234567890']])
assert c.invokefunction('getRowId', [user, table_name]) == 3
assert c.invokefunction('getColumnTypes', [user, table_name]) == column_types.decode()
assert c.invokefunction('getRows', [user, table_name, [1, 2]]) == [[False, 23333, 'test str', 2147483647, '0123456789abcdef0123456789abcdef'], [True, 233, 'test str 233', 2147483646, '12345678901234567890\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00']]

assert c.invokefunction('getRow', [user, table_name, 1]) == [False, 23333, 'test str', 2147483647, '0123456789abcdef0123456789abcdef']
assert c.invokefunction('getRow', [user, table_name, 2]) == [True, 233, 'test str 233', 2147483646, '12345678901234567890\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00']
assert 'No data' in c.invokefunction('getRow', [user, table_name, 0], do_not_raise_on_result=True)
assert len(c.invokefunction('listRows', [user, table_name])) == 2

assert c.invokefunction('splayGetSize', [user, table_name, 2]) == 2
assert c.invokefunction('splayGetRoot', [user, table_name, 2]) == 233

v=23333
assert int.from_bytes(c.invokefunction('splayGetParent', [user, table_name, 2, v]), "little", signed=True) == 233
assert c.invokefunction('splayGetLeft', [user, table_name, 2, v]) == None
assert c.invokefunction('splayGetRight', [user, table_name, 2, v]) == None
v=233
assert c.invokefunction('splayGetParent', [user, table_name, 2, v]) == None
assert c.invokefunction('splayGetLeft', [user, table_name, 2, v]) == None
assert int.from_bytes(c.invokefunction('splayGetRight', [user, table_name, 2, v]).encode(), "little", signed=True) == 23333

assert int.from_bytes(c.invokefunction('splayPredecessor', [user, table_name, 2, 23333]), "little", signed=True) == 233
assert c.invokefunction('splaySuccessor', [user, table_name, 2, 23333]) == None
assert c.invokefunction('splayPredecessor', [user, table_name, 2, 233]) == None
assert int.from_bytes(c.invokefunction('splaySuccessor', [user, table_name, 2, 233]).encode(), "little", signed=True) == 23333
assert int.from_bytes(c.invokefunction('splayMax', [user, table_name, 2, None]).encode(), "little", signed=True) == 23333
assert int.from_bytes(c.invokefunction('splayMin', [user, table_name, 2, None]), "little", signed=True) == 233

c.invokefunction('deleteRow', [user, table_name, 1])
c.invokefunction('deleteRow', [user, table_name, 2])
assert len(c.invokefunction('listRows', [user, table_name])) == 0
c.invokefunction('deleteRow', [user, table_name, 0])  # no id 0, but should not raise error
assert len(c.invokefunction('listRows', [user, table_name])) == 0

c.invokefunction('dropTable', [user, table_name])
assert 'No table' in c.invokefunction('getRow', [user, table_name, 1], do_not_raise_on_result=True)
assert len(c.invokefunction('listTables', [user, ''])) == 0
assert len(c.invokefunction('listAllTables')) == 0
assert len(c.invokefunction('listDroppedTables', [user, ''])) == 1
assert len(c.invokefunction('listAllDroppedTables')) == 1
assert c.invokefunction('getColumnTypes', [user, table_name]) == None
assert c.invokefunction('getColumnTypesDropped', [user, table_name]) == column_types.decode()

# be aware that Neo3 allows only storage key <= 64 bytes
table_name = 'customPrimaryKey'
assert c.invokefunction(
    'createTable', [
        user, table_name,
        Types.UINT160 + Types.UINT256 + Types.IntFixedLen + b'\x04',
        False
    ]) == 3
data = [
    [Hash160Str('0x'+'00'*20), Hash256Str('0x'+'00'*32), 1],
    [Hash160Str('0x'+'00'*19+'01'), Hash256Str('0x'+'00'*31+'01'), -2],
    [Hash160Str('0x'+'00'*19+'02'), Hash256Str('0x'+'00'*31+'02'), -3],
]

c.copy_snapshot(main_session, single_write_session := 'singleWrite')
c.fairy_session = single_write_session
for row in data:
    c.invokefunction('writeRow', [user, table_name, row])
assert c.invokefunction('getRow', [user, table_name, data[0][0]]) == ['\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', 1]
assert c.invokefunction('getRow', [user, table_name, data[1][0]]) == ['\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', -2]
assert c.invokefunction('getRow', [user, table_name, data[2][0]]) == ['\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', -3]
assert c.invokefunction('findPrimaryKeyFromValue', [user, table_name, 3, 1]) == ['\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00']
c.invokefunction('writeRow', [user, table_name, [Hash160Str('0x'+'00'*19+'03'), Hash256Str('0x'+'00'*31+'03'), -3]])
assert set(c.invokefunction('findPrimaryKeyFromValue', [user, table_name, 3, -3])) == {'\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'}
r"""
-3
 -2
   1
"""
v=1
assert int.from_bytes(c.invokefunction('splayGetParent', [user, table_name, 3, v]), "little", signed=True) == -2
assert c.invokefunction('splayGetLeft', [user, table_name, 3, v]) == None
assert c.invokefunction('splayGetRight', [user, table_name, 3, v]) == None
v=-2
assert int.from_bytes(c.invokefunction('splayGetParent', [user, table_name, 3, v]), "little", signed=True) == -3
assert c.invokefunction('splayGetLeft', [user, table_name, 3, v]) == None
assert int.from_bytes(c.invokefunction('splayGetRight', [user, table_name, 3, v]).encode(), "little", signed=True) == 1
v=-3
assert c.invokefunction('splayGetParent', [user, table_name, 3, v]) == None
assert c.invokefunction('splayGetLeft', [user, table_name, 3, v]) == None
assert int.from_bytes(c.invokefunction('splayGetRight', [user, table_name, 3, v]), "little", signed=True) == -2
assert c.invokefunction('splaySuccessor', [user, table_name, 3, -4]) == b'\xfd'
assert c.invokefunction('splaySuccessor', [user, table_name, 3, -3]) == b'\xfe'
assert c.invokefunction('splaySuccessor', [user, table_name, 3, -2]) == '\x01'
assert c.invokefunction('splaySuccessor', [user, table_name, 3, -1]) == '\x01'
assert c.invokefunction('splaySuccessor', [user, table_name, 3, 0]) == '\x01'
assert c.invokefunction('splaySuccessor', [user, table_name, 3, 1]) == None
assert c.invokefunction('splaySuccessor', [user, table_name, 3, 2]) == None
assert c.invokefunction('splayPredecessor', [user, table_name, 3, -4]) == None
assert c.invokefunction('splayPredecessor', [user, table_name, 3, -3]) == None
assert c.invokefunction('splayPredecessor', [user, table_name, 3, -2]) == b'\xfd'
assert c.invokefunction('splayPredecessor', [user, table_name, 3, -1]) == b'\xfe'
assert c.invokefunction('splayPredecessor', [user, table_name, 3, 0]) == b'\xfe'
assert c.invokefunction('splayPredecessor', [user, table_name, 3, 1]) == b'\xfe'
assert c.invokefunction('splayPredecessor', [user, table_name, 3, 2]) == '\x01'
c.fairy_session = main_session

c.invokefunction('writeRows', [user, table_name, data])
assert c.invokefunction('getRow', [user, table_name, data[0][0]]) == ['\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', 1]
assert c.invokefunction('getRow', [user, table_name, data[1][0]]) == ['\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', -2]
assert c.invokefunction('getRow', [user, table_name, data[2][0]]) == ['\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', -3]
assert c.invokefunction('getRows', [user, table_name, [entry[0] for entry in data]]) == [
    ['\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', 1],
    ['\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', -2],
    ['\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', -3]
]
assert len(rows := c.invokefunction('listRows', [user, table_name])) == 3

c.invokefunction('writeRows', [user, table_name, data])  # write again to test splay tree
assert int.from_bytes(c.invokefunction('splayPredecessor', [user, table_name, 3, 1]), "little", signed=True) == -2
assert c.invokefunction('splaySuccessor', [user, table_name, 3, 1]) == None
assert int.from_bytes(c.invokefunction('splayPredecessor', [user, table_name, 3, -2]), "little", signed=True) == -3
assert int.from_bytes(c.invokefunction('splaySuccessor', [user, table_name, 3, -2]).encode(), "little", signed=True) == 1
assert c.invokefunction('splayPredecessor', [user, table_name, 3, -3]) == None
assert int.from_bytes(c.invokefunction('splaySuccessor', [user, table_name, 3, -3]), "little", signed=True) == -2

c.invokefunction('deleteRow', [user, table_name, data[0][0]])
c.invokefunction('writeRow', [user, table_name, data[1]])
# print(rows)
c.invokefunction('deleteRows', [user, table_name, [entry[0] for entry in data]])
assert len(c.invokefunction('listRows', [user, table_name])) == 0

table_name = 'FixedLenPrimaryKey'
assert c.invokefunction(
    'createTable', [
        user, table_name,
        Types.IntFixedLen + b'\x04' + Types.IntFixedLen + b'\x04',
        False
    ]) == 2
c.invokefunction('writeRows', [user, table_name, data := [
    [0x04030201, -1],
    [0x04030202, -2],
    [0x04030203, -3],
]])
assert c.invokefunction('getRows', [user, table_name, [d[0] for d in data]]) == data
c.invokefunction('writeRow', [user, table_name, [0x04030204, -4]])
assert c.invokefunction('getRow', [user, table_name, data[0][0]]) == data[0]
assert c.invokefunction('getRow', [user, table_name, 0x04030204]) == [0x04030204, -4]

coverage = {k: v for k, v in c.get_contract_source_code_coverage().items() if 'Undefined' not in k}
opcode_count = sum(len(v) for v in coverage.values())
uncovered = {k: {opcode: covered for opcode, covered in v.items() if covered == False} for k, v in coverage.items()
             if 'throw new' not in k
             and 'default:' not in k
             and 'ExecutionEngine.Assert' not in k
             and '::line 0' not in k
             and not all(v.values())}
uncovered_count = sum(len(v) for v in uncovered.values())
uncovered = dict(sorted(uncovered.items()))
for k, v in uncovered.items():
    print(f"{k}:")
    print(f"\t{v}")
print(f'Coverage: {(opcode_count-uncovered_count)/opcode_count}=={opcode_count-uncovered_count}/{opcode_count}')
