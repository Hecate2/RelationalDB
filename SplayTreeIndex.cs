using System;
using System.ComponentModel;
using System.Numerics;
using Neo;
using Neo.SmartContract.Framework;
using Neo.SmartContract.Framework.Attributes;
using Neo.SmartContract.Framework.Native;
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
        const byte SPLAY_SIZE = 0xe0;               // size of splay tree
        const byte SPLAY_ROOT = 0xef;               // KEY of root node
        const byte SPLAY_NODE_PARENT_PREFIX = 0xf0; // KEY of current node -> KEY of parent node
        const byte SPLAY_NODE_LEFT_PREFIX = 0xf1;   // KEY of current node -> KEY of the left child
        const byte SPLAY_NODE_RIGHT_PREFIX = 0xf2;  // KEY of current node -> KEY of the right child
        const byte SPLAY_NODE_COUNT_PREFIX = 0xf3;  // KEY of current node -> the count of current node
        // KEY of node == UInt160 user + ByteString tableName + SEPARATOR + 1-byte columnId + value

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
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT);

            ByteString y = rightMap[x];
            ByteString xParent = parentMap[x];
            if (y != null)
            {
                ByteString yLeft = leftMap[y];
                rightMap[x] = yLeft;
                if (yLeft != null)
                    parentMap[yLeft] = x;
                parentMap[y] = xParent;
            }

            if (xParent != null)
                rootMap[columnKey] = y;
            else if (x == leftMap[xParent])
                leftMap[xParent] = y;
            else
                rightMap[xParent] = y;
            if (y != null)
                leftMap[y] = x;
            parentMap[x] = y;
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
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT);

            ByteString y = leftMap[x];
            ByteString xParent = parentMap[x];
            if (y != null)
            {
                ByteString yRight = rightMap[y];
                leftMap[x] = yRight;
                if (yRight != null)
                    parentMap[yRight] = x;
                parentMap[y] = xParent;
            }

            if (xParent != null)
                rootMap[columnKey] = y;
            else if (x == leftMap[xParent])
                leftMap[xParent] = y;
            else
                rightMap[xParent] = y;
            if (y != null)
                rightMap[y] = x;
            parentMap[x] = y;
        }

        /// <summary>
        /// Splays x to be a child of subtreeRoot. Use subtreeRoot=null to let x be the root of the whole tree
        /// </summary>
        /// <param name="columnKey">user + tableName + SEPARATOR + (ByteString)new byte[] { columnId };</param>
        /// <param name="x"></param>
        /// <param name="subtreeRoot">use null to let x be the root of the whole tree</param>
        protected void Splay(ByteString columnKey, ByteString x, ByteString subtreeRoot)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap parentMap = new(context, (ByteString)new byte[] { SPLAY_NODE_PARENT_PREFIX } + columnKey);
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);

            ByteString xParent = parentMap[x];
            while (xParent != subtreeRoot)
            {
                ByteString xGrandParent = parentMap[xParent];
                if (xGrandParent != subtreeRoot)
                {
                    if (leftMap[xParent] == x)
                        SplayRightRotateZig(columnKey, xParent);
                    else
                        SplayLeftRotateZag(columnKey, xParent);
                }
                else if (leftMap[xParent] == x && leftMap[xGrandParent] == xParent)
                {
                    SplayRightRotateZig(columnKey, xGrandParent);
                    SplayRightRotateZig(columnKey, xParent);
                }
                else if (rightMap[xParent] == x && rightMap[xGrandParent] == xParent)
                {
                    SplayLeftRotateZag(columnKey, xParent);
                    SplayLeftRotateZag(columnKey, xParent);
                }
                else if (leftMap[xParent] == x && rightMap[xGrandParent] == xParent)
                {
                    SplayRightRotateZig(columnKey, xParent);
                    SplayLeftRotateZag(columnKey, xParent);
                }
                else
                {
                    SplayLeftRotateZag(columnKey, xParent);
                    SplayRightRotateZig(columnKey, xParent);
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
        protected void Insert(UInt160 user, ByteString tableName, byte columnId, ByteString x) => Insert(user + tableName + SEPARATOR + (ByteString)new byte[] { columnId }, x);
        protected void Insert(ByteString columnKey, ByteString x)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap treeSizeMap = new(context, SPLAY_SIZE);
            BigInteger treeSize = (BigInteger)treeSizeMap[columnKey];
            treeSizeMap.Put(columnKey, treeSize + 1);
            StorageMap nodeCountMap = new(context, (ByteString)new byte[] { SPLAY_NODE_COUNT_PREFIX } + columnKey);
            BigInteger nodeCount = (BigInteger)nodeCountMap[x];
            nodeCountMap.Put(x, nodeCount + 1);
            if (nodeCount > 1)  // x already in tree
            {
                Splay(columnKey, x, null);
                return;
            }
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT);
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
                        parentMap[x] = u;
                        leftMap[u] = x;
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
                        parentMap[x] = u;
                        rightMap[u] = x;
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
            ExecutionEngine.Assert(nodeCount > 0, "No value " + x);
            Splay(columnKey, x, null);
            return nodeCount;
        }

        public ByteString SplayMax(UInt160 user, ByteString tableName, byte columnId, ByteString subtreeRoot) => SplayMax(user + tableName + SEPARATOR +(ByteString)new byte[] { columnId }, subtreeRoot);
        public ByteString SplayMax(ByteString columnKey, ByteString subtreeRoot)
        {
            StorageContext context = Storage.CurrentContext;
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
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
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT);
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
                        ansValue = (BigInteger)ans;
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
            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT);
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
                        ansValue = (BigInteger)ans;
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
            ExecutionEngine.Assert(nodeCount > 0, "No value " + x);
            nodeCountMap.Put(x, nodeCount - 1);
            StorageMap treeSizeMap = new(context, SPLAY_SIZE);
            BigInteger treeSize = (BigInteger)treeSizeMap[columnKey];
            treeSizeMap.Put(columnKey, treeSize - 1);

            Splay(columnKey, x, null);

            StorageMap rootMap = new StorageMap(context, SPLAY_ROOT);
            StorageMap parentMap = new(context, (ByteString)new byte[] { SPLAY_NODE_PARENT_PREFIX } + columnKey);
            StorageMap leftMap = new(context, (ByteString)new byte[] { SPLAY_NODE_LEFT_PREFIX } + columnKey);
            StorageMap rightMap = new(context, (ByteString)new byte[] { SPLAY_NODE_RIGHT_PREFIX } + columnKey);
            ByteString xLeft = leftMap[x];
            ByteString xRight = rightMap[x];
            if (xLeft != null)
            {
                rootMap[columnKey] = xRight;
                if (xRight != null)
                    parentMap[xRight] = null;
                return;
            }
            Splay(columnKey, SplayMax(columnKey, xLeft), rootMap[columnKey]);
            parentMap[xLeft] = null;
            if (xRight != null)
            {
                rightMap[xLeft] = xRight;
                parentMap[xRight] = xLeft;
            }
            rootMap[columnKey] = xLeft;
            return;
        }
    }
}
