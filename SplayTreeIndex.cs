using System.Numerics;
using Neo;
using Neo.SmartContract.Framework;
using Neo.SmartContract.Framework.Services;

namespace RelationalDB
{
    /// <summary>
    /// Here we are going to implement a splay tree
    /// in order to implement secondary indexes and range query (SELECT * FROM table WHERE num > 5)
    /// Given that we have `Find`, we can `SELECT * FROM table WHERE num == 5` very quickly, with
    /// key = user+tableName+num+primaryKey -> num = NULL
    /// And then for range query, we just need to find the largest value less than num,
    /// and the smallest number greater than num.
    /// All the nodes in the left subtree is smaller than the current node.
    /// All the nodes in the right subtree is larger than the current node.
    /// For now, we do not always splay the recently used nodes to the root of the tree.
    /// Thanks to https://www.luogu.com.cn/blog/threebody/post-mu-ban-splay-shen-zhan-shu-suan-fa-yang-xie
    /// </summary>
    public partial class RelationalDB
    {
        // columnKey == UInt160 user + ByteString tableName + SEPARATOR + 1-byte columnId
        const byte SPLAY_SIZE_PREFIX = 0xe0;               // columnKey -> size of splay tree
        const byte SPLAY_ROOT_PREFIX = 0xef;               // columnKey -> value of root node
        const byte SPLAY_NODE_PARENT_PREFIX = 0xf0; // columnKey + current node -> parent node
        const byte SPLAY_NODE_LEFT_PREFIX = 0xf1;   // columnKey + current node -> left child
        const byte SPLAY_NODE_RIGHT_PREFIX = 0xf2;  // columnKey + current node -> right child
        const byte SPLAY_NODE_COUNT_PREFIX = 0xf3;  // columnKey + current node -> the count of current node

        public BigInteger SplayGetSize(UInt160 user, ByteString tableName, byte columnId) => (BigInteger)new StorageMap(SPLAY_SIZE_PREFIX)[user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }];
        public BigInteger SplayGetRoot(UInt160 user, ByteString tableName, byte columnId) => (BigInteger)new StorageMap(SPLAY_ROOT_PREFIX)[user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }];
        public ByteString SplayGetParent(UInt160 user, ByteString tableName, byte columnId, ByteString value) => new StorageMap(SPLAY_NODE_PARENT_PREFIX)[user + tableName + SEPARATOR + (ByteString)new byte[] { columnId } + value];
        public ByteString SplayGetLeft(UInt160 user, ByteString tableName, byte columnId, ByteString value) => new StorageMap(SPLAY_NODE_LEFT_PREFIX)[user + tableName + SEPARATOR +(ByteString)new byte[] { columnId } + value];
        public ByteString SplayGetRight(UInt160 user, ByteString tableName, byte columnId, ByteString value) => new StorageMap(SPLAY_NODE_RIGHT_PREFIX)[user + tableName + SEPARATOR +(ByteString)new byte[] { columnId } + value];
        public BigInteger SplayGetNodeCount(UInt160 user, ByteString tableName, byte columnId, ByteString value) => (BigInteger)new StorageMap(SPLAY_NODE_COUNT_PREFIX)[user + tableName + SEPARATOR + (ByteString)new byte[] { columnId } + value];

        public void SplayDebugPut(UInt160 user, ByteString tableName, byte columnId,
            ByteString node, ByteString parent, ByteString leftChild, ByteString rightChild)
        {
            ExecutionEngine.Assert(user == UInt160.Zero || Runtime.CheckWitness(user), "debugPut witness");
            StorageContext context = Storage.CurrentContext;
            ByteString columnKey = user + tableName + SEPARATOR + (ByteString)new byte[] { columnId };
            SplayPut(new(context, (ByteString)new byte[] { SPLAY_NODE_PARENT_PREFIX } + columnKey), node, parent);
            SplayPut(new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey), node, leftChild);
            SplayPut(new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey), node, rightChild);
        }
        public void SplayDebugLeftRotateZag(UInt160 user, ByteString tableName, byte columnId, ByteString node)
        {
            ExecutionEngine.Assert(user == UInt160.Zero || Runtime.CheckWitness(user), "debugLeft witness");
            ByteString columnKey = user + tableName + SEPARATOR + (ByteString)new byte[] { columnId };
            SplayLeftRotateZag(columnKey, node);
        }
        public void SplayDebugRightRotateZig(UInt160 user, ByteString tableName, byte columnId, ByteString node)
        {
            ExecutionEngine.Assert(user == UInt160.Zero || Runtime.CheckWitness(user), "debugRight witness");
            ByteString columnKey = user + tableName + SEPARATOR + (ByteString)new byte[] { columnId };
            SplayRightRotateZig(columnKey, node);
        }
        public void SplayDebugSplay(UInt160 user, ByteString tableName, byte columnId, ByteString node, ByteString subtreeRoot)
        {
            ExecutionEngine.Assert(user == UInt160.Zero || Runtime.CheckWitness(user), "debugSplay witness");
            ByteString columnKey = user + tableName + SEPARATOR + (ByteString)new byte[] { columnId };
            Splay(columnKey, node, subtreeRoot);
        }

        protected void SplayPut(StorageMap map, ByteString key, ByteString value)
        {
            if (key == null)
                return;
            if (value == null)
                map.Delete(key);
            else
                map.Put(key, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnKey">user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }</param>
        /// <param name="x"></param>
        protected void SplayLeftRotateZag(ByteString columnKey, ByteString x)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap parentMap = new(context, (ByteString)new byte[] { SPLAY_NODE_PARENT_PREFIX } + columnKey);
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT_PREFIX);

            ByteString xParent = parentMap[x];
            ByteString xLeft = leftMap[x];
            ByteString xGrandParent = xParent == null ? null : parentMap[xParent];
            SplayPut(rightMap, xParent, xLeft);
            SplayPut(parentMap, xLeft, xParent);
            xParent = parentMap[x];
            SplayPut(leftMap, x, xParent);
            SplayPut(parentMap, xParent, x);
            SplayPut(parentMap, x, xGrandParent);
            if (xGrandParent == null)
            {
                rootMap[columnKey] = x;
                return;
            }
            if ((BigInteger)x < (BigInteger)xGrandParent)
                SplayPut(leftMap, xGrandParent, x);
            else
                SplayPut(rightMap, xGrandParent, x);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnKey">user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }</param>
        /// <param name="x"></param>
        protected void SplayRightRotateZig(ByteString columnKey, ByteString x)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap parentMap = new(context, (ByteString)new byte[] { SPLAY_NODE_PARENT_PREFIX } + columnKey);
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT_PREFIX);

            ByteString xParent = parentMap[x];
            ByteString xRight = rightMap[x];
            ByteString xGrandParent = parentMap[xParent];
            SplayPut(leftMap, xParent, xRight);
            SplayPut(parentMap, xRight, xParent);
            xParent = parentMap[x];
            SplayPut(rightMap, x, xParent);
            SplayPut(parentMap, xParent, x);
            SplayPut(parentMap, x, xGrandParent);
            if (xGrandParent == null)
            {
                rootMap[columnKey] = x;
                return;
            }
            if ((BigInteger)x < (BigInteger)xGrandParent)
                SplayPut(leftMap, xGrandParent, x);
            else
                SplayPut(rightMap, xGrandParent, x);
        }

        /// <summary>
        /// Splays x to be a child of subtreeRoot. Use subtreeRoot=null to let x be the root of the whole tree
        /// </summary>
        /// <param name="columnKey">user + tableName + SEPARATOR + (ByteString)new byte[] { columnId };</param>
        /// <param name="x"></param>
        /// <param name="subtreeRoot">use null to let x be the root of the whole tree</param>
        protected void Splay(ByteString columnKey, ByteString x, ByteString subtreeRoot)
        {
            BigInteger xValue = (BigInteger)x;  // x may be recognized as BigInteger, though defined as ByteString
            StorageContext context = Storage.CurrentContext;
            StorageMap parentMap = new(context, (ByteString)new byte[] { SPLAY_NODE_PARENT_PREFIX } + columnKey);
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            //StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);

            ByteString xParent = parentMap[x];
            while (xParent != subtreeRoot)
            {
                ByteString xGrandParent = parentMap[xParent];
                if (xGrandParent == subtreeRoot)
                {
                    if ((BigInteger)leftMap[xParent] == xValue)
                        SplayRightRotateZig(columnKey, x);
                    else
                        SplayLeftRotateZag(columnKey, x);
                    return;
                }
                else
                {
                    if ((BigInteger)leftMap[xParent] == xValue)
                    {
                        if (leftMap[xGrandParent] == xParent)
                        {
                            SplayRightRotateZig(columnKey, xParent);
                            SplayRightRotateZig(columnKey, x);
                        }
                        else
                        {
                            SplayRightRotateZig(columnKey, x);
                            SplayLeftRotateZag(columnKey, x);
                        }
                    }
                    else
                    {
                        if (leftMap[xGrandParent] == xParent)
                        {
                            SplayLeftRotateZag(columnKey, x);
                            SplayRightRotateZig(columnKey, x);
                        }
                        else
                        {
                            SplayLeftRotateZag(columnKey, xParent);
                            SplayLeftRotateZag(columnKey, x);
                        }
                    }
                }
                xParent = parentMap[x];
            }
        }

        /// <summary>
        /// Insert x and splay x to the root
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="columnId"></param>
        /// <param name="x"></param>
        protected void SplayInsert(UInt160 user, ByteString tableName, byte columnId, ByteString x) => SplayInsert(user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }, x);
        protected void SplayInsert(ByteString columnKey, ByteString x)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap treeSizeMap = new(context, SPLAY_SIZE_PREFIX);
            BigInteger treeSize = (BigInteger)treeSizeMap[columnKey];
            treeSizeMap.Put(columnKey, treeSize + 1);
            StorageMap nodeCountMap = new(context, (ByteString)new byte[] { SPLAY_NODE_COUNT_PREFIX } + columnKey);
            BigInteger nodeCount = (BigInteger)nodeCountMap[x];
            nodeCountMap.Put(x, nodeCount + 1);
            if (nodeCount >= 1)  // x already in tree
            {
                Splay(columnKey, x, null);
                return;
            }
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT_PREFIX);
            StorageMap parentMap = new(context, (ByteString)new byte[] { SPLAY_NODE_PARENT_PREFIX } + columnKey);
            ByteString root = rootMap[columnKey];
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
            if (root == null)  // nothing inserted before
            {
                rootMap[columnKey] = x;
                //parentMap[x] = null;
                //leftMap[x] = null;
                //rightMap[x] = null;
                return;
            }

            ByteString u = root;
            BigInteger xValue = (BigInteger)x;
            while (true)
            {
                if (xValue < (BigInteger)u)
                {
                    ByteString left = leftMap[u];
                    if (left != null)
                        u = left;
                    else
                    {
                        SplayPut(parentMap, x, u);
                        SplayPut(leftMap, u, x);
                        break;
                    }
                }
                else
                {
                    ByteString right = rightMap[u];
                    if (right != null)
                        u = right;
                    else
                    {
                        SplayPut(parentMap, x, u);
                        SplayPut(rightMap, u, x);
                        break;
                    }
                }
            }
            Splay(columnKey, x, null);
        }

        /// <summary>
        /// Find and splay
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="columnId"></param>
        /// <param name="subtreeRoot"></param>
        /// <returns></returns>
        public BigInteger SplayFind(UInt160 user, ByteString tableName, byte columnId, ByteString x) => SplayFind(user + tableName + SEPARATOR +(ByteString)new byte[] { columnId }, x);
        public BigInteger SplayFind(ByteString columnKey, ByteString x)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap nodeCountMap = new(context, (ByteString)new byte[] { SPLAY_NODE_COUNT_PREFIX } + columnKey);
            BigInteger nodeCount = (BigInteger)nodeCountMap[x];
            ExecutionEngine.Assert(nodeCount > 0, "No value");
            Splay(columnKey, x, null);
            return nodeCount;
        }

        public ByteString SplayMax(UInt160 user, ByteString tableName, byte columnId, ByteString subtreeRoot) => SplayMax(user + tableName + SEPARATOR +(ByteString)new byte[] { columnId }, subtreeRoot);
        public ByteString SplayMax(ByteString columnKey, ByteString subtreeRoot)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
            if (subtreeRoot == null)
                subtreeRoot = new StorageMap(context, SPLAY_ROOT_PREFIX)[columnKey];
            while (true)
            {
                ByteString right = rightMap[subtreeRoot];
                if (right == null)
                    return subtreeRoot;
                subtreeRoot = right;
            }
        }

        public ByteString SplayMin(UInt160 user, ByteString tableName, byte columnId, ByteString subtreeRoot) => SplayMin(user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }, subtreeRoot);
        public ByteString SplayMin(ByteString columnKey, ByteString subtreeRoot)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            if (subtreeRoot == null)
                subtreeRoot = new StorageMap(context, SPLAY_ROOT_PREFIX)[columnKey];
            while (true)
            {
                ByteString left = leftMap[subtreeRoot];
                if (left == null)
                    return subtreeRoot;
                subtreeRoot = left;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="columnId"></param>
        /// <param name="x">The greatest num in the tree less than x. We allow x to be a number not inserted into the tree</param>
        /// <returns>null if no predecessor</returns>
        public ByteString SplayPredecessor(UInt160 user, ByteString tableName, byte columnId, ByteString x) => SplayPredecessor(user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }, x);
        public ByteString SplayPredecessor(ByteString columnKey, ByteString x)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT_PREFIX);
            ByteString p = rootMap[columnKey];
            BigInteger xValue = (BigInteger)x;
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
            BigInteger ansValue = 0;
            ByteString ans = null;
            while (p != null)
            {
                BigInteger pValue = (BigInteger)p;
                if (pValue >= xValue)
                    p = leftMap[p];
                else  // p < x; p may be the answer
                {
                    if (ans == null || ansValue < pValue)
                    {
                        ans = p;
                        ansValue = pValue;
                    }
                    p = rightMap[p];
                }
            }
            return ans;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="columnId"></param>
        /// <param name="x">The smallest num in the tree greater than x. We allow x to be a number not inserted into the tree</param>
        /// <returns>null if no predecessor</returns>
        public ByteString SplaySuccessor(UInt160 user, ByteString tableName, byte columnId, ByteString x) => SplaySuccessor(user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }, x);
        public ByteString SplaySuccessor(ByteString columnKey, ByteString x)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT_PREFIX);
            ByteString p = rootMap[columnKey];
            BigInteger xValue = (BigInteger)x;
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
            BigInteger ansValue = 0;
            ByteString ans = null;
            while (p != null)
            {
                BigInteger pValue = (BigInteger)p;
                if (pValue <= xValue)
                    p = rightMap[p];
                else  // p > x; p may be the answer
                {
                    if (ans == null || ansValue > pValue)
                    {
                        ans = p;
                        ansValue = pValue;
                    }
                    p = leftMap[p];
                }
            }
            return ans;
        }

        protected void SplayDelete(UInt160 user, ByteString tableName, byte columnId, ByteString x) => SplayDelete(user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }, x);
        protected void SplayDelete(ByteString columnKey, ByteString x)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap nodeCountMap = new(context, (ByteString)new byte[] { SPLAY_NODE_COUNT_PREFIX } + columnKey);
            BigInteger nodeCount = (BigInteger)nodeCountMap[x];
            ExecutionEngine.Assert(nodeCount > 0, "No value");
            nodeCountMap.Put(x, nodeCount - 1);
            StorageMap treeSizeMap = new(context, SPLAY_SIZE_PREFIX);
            BigInteger treeSize = (BigInteger)treeSizeMap[columnKey];
            treeSizeMap.Put(columnKey, treeSize - 1);

            Splay(columnKey, x, null);

            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT_PREFIX);
            StorageMap parentMap = new(context, (ByteString)new byte[] { SPLAY_NODE_PARENT_PREFIX } + columnKey);
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
            ByteString xLeft = leftMap[x];
            ByteString xRight = rightMap[x];
            if (xLeft == null)
            {
                SplayPut(rootMap, columnKey, xRight);
                if (xRight != null)
                    parentMap.Delete(xRight);
                return;
            }
            Splay(columnKey, SplayMax(columnKey, xLeft), rootMap[columnKey]);
            xLeft = leftMap[x];
            if (xLeft != null)
                parentMap.Delete(xLeft);
            if (xRight != null)
            {
                SplayPut(rightMap, xLeft, xRight);
                SplayPut(parentMap, xRight, xLeft);
            }
            SplayPut(rootMap, columnKey, xLeft);
            return;
        }
    }
}
