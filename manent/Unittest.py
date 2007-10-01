import unittest

test_loader = unittest.TestLoader()

from testsuite.TestPacker import TestPacker
suite_Packer = test_loader.loadTestsFromTestCase(TestPacker)

from testsuite.TestContainer import TestContainer
suite_Container = test_loader.loadTestsFromTestCase(TestContainer)

from testsuite.TestIncrement import TestIncrement
suite_Increment = test_loader.loadTestsFromTestCase(TestIncrement)

from testsuite.TestFormat import TestFormat
suite_Format = test_loader.loadTestsFromTestCase(TestFormat)

from testsuite.TestBlock import TestBlock
suite_Block = test_loader.loadTestsFromTestCase(TestBlock)

from testsuite.TestDatabase import TestDatabase
suite_DB = test_loader.loadTestsFromTestCase(TestDatabase)

from testsuite.TestNodes import TestNodes
suite_Nodes = test_loader.loadTestsFromTestCase(TestNodes)

suite = unittest.TestSuite([suite_Packer,suite_Container,suite_Nodes, suite_Increment, suite_DB])
#suite = unittest.TestSuite([suite_Nodes, suite_ITree, suite_Format, suite_Block, suite_DB])
unittest.TextTestRunner(verbosity=2).run(suite)
