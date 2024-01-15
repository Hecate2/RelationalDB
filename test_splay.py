from neo_fairy_client import FairyClient, Hash160Str, Hash256Str
user = Hash160Str('0xb1983fa2479a0c8e2beae032d2df564b5451b7a5')
c = FairyClient(fairy_session='splay', wallet_address_or_scripthash=user, with_print=False)
c.virutal_deploy_from_path('./bin/sc/RelationalDB.nef')
table_name = 'rotate'
column_id = 0x01
r'''
   1
  / \
 2   3
4 5
'''
c.invokefunction('splayDebugPut', [user, table_name, column_id, 1, None, 2, 3])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 2, 1, 4, 5])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 3, 1, None, None])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 4, 2, None, None])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 5, 2, None, None])
for v in [1,2,3,4,5]:
    print(v)
    print(c.invokefunction('splayGetParent', [user, table_name, column_id, v]), end=' ')
    print(c.invokefunction('splayGetLeft', [user, table_name, column_id, v]), end=' ')
    print(c.invokefunction('splayGetRight', [user, table_name, column_id, v]))
c.invokefunction('splayDebugRightRotateZig', [user, table_name, column_id, 2])
for v in [1,2,3,4,5]:
    print(v)
    print(c.invokefunction('splayGetParent', [user, table_name, column_id, v]), end=' ')
    print(c.invokefunction('splayGetLeft', [user, table_name, column_id, v]), end=' ')
    print(c.invokefunction('splayGetRight', [user, table_name, column_id, v]))
print(table_name)
c.invokefunction('splayDebugLeftRotateZag', [user, table_name, column_id, 1])
for v in [1,2,3,4,5]:
    print(v)
    print(c.invokefunction('splayGetParent', [user, table_name, column_id, v]), end=' ')
    print(c.invokefunction('splayGetLeft', [user, table_name, column_id, v]), end=' ')
    print(c.invokefunction('splayGetRight', [user, table_name, column_id, v]))
print('----------')

r'''
         g
        / \
       p   D
      / \
     x   C
    A B
'''
table_name = 'splay'
c.invokefunction('splayDebugPut', [user, table_name, column_id, 'g', None, 'p', 'D'])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 'p', 'g', 'x', 'C'])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 'x', 'p', 'A', 'B'])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 'A', 'x', None, None])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 'B', 'x', None, None])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 'C', 'p', None, None])
c.invokefunction('splayDebugPut', [user, table_name, column_id, 'D', 'g', None, None])
for v in ['g','p','x','A','B','C','D']:
    print(v)
    result = c.invokefunction('splayGetParent', [user, table_name, column_id, v])
    print(chr(result) if result else 0, end=' ')
    result = c.invokefunction('splayGetLeft', [user, table_name, column_id, v])
    print(chr(result) if result else 0, end=' ')
    result = c.invokefunction('splayGetRight', [user, table_name, column_id, v])
    print(chr(result) if result else 0)
print(table_name)
c.invokefunction('splayDebugSplay', [user, table_name, column_id, 'x', None])
r'''
     x
    / \
   A   p
      / \
     B   g
        C D
'''
for v in ['g','p','x','A','B','C','D']:
    print(v)
    result = c.invokefunction('splayGetParent', [user, table_name, column_id, v])
    print(chr(result) if result else 0, end=' ')
    result = c.invokefunction('splayGetLeft', [user, table_name, column_id, v])
    print(chr(result) if result else 0, end=' ')
    result = c.invokefunction('splayGetRight', [user, table_name, column_id, v])
    print(chr(result) if result else 0)

print('----------')
table_name = 'splay'