/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jinterop.dcom.core.IJIComObject;
import org.jinterop.dcom.core.JIArray;
import org.jinterop.dcom.core.JICallBuilder;
import org.jinterop.dcom.core.JIString;
import org.junit.jupiter.api.Test;
import org.openscada.opc.dcom.common.impl.EnumString;
import org.openscada.opc.lib.da.browser.Branch;
import org.openscada.opc.lib.da.browser.Leaf;

class OpcDaBrowseSupportTest {

    @Test
    void treeScanIncludesRootLeavesAndAllPrefilledChildren() throws Exception {
        Branch root = new Branch();
        root.setLeaves(List.of(new Leaf(root, "root", "root-id")));
        List<Branch> children = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            Branch child = branch(root, "branch-" + i);
            child.setLeaves(List.of(new Leaf(child, "value", "id-" + i)));
            children.add(child);
        }
        root.setBranches(children);

        var scan =
                OpcDaBrowseSupport.scanTree(
                        root, new PrefilledTree(), new OpcDaBrowseSupport.TreeLimits(1_000, 1_000));

        assertEquals(12, scan.itemIds().size());
        assertEquals(12, scan.processedBranches());
        scan.requireComplete();
    }

    @Test
    void utgardEnumStringRequestsFullBatchesUntilAFinalShortBatch() throws Exception {
        var fake = new FakeEnumString(List.of("a", "b", "c", "d", "e"));

        var actual = new EnumString(fake.comObject()).asCollection(2);

        assertEquals(List.of("a", "b", "c", "d", "e"), List.copyOf(actual));
        assertEquals(List.of(2, 2, 2), fake.requests());
        assertEquals(List.of(2, 2, 1), fake.returnedCounts());
        assertEquals(1, fake.resetCount());
    }

    @Test
    void utgardEnumStringRequestsAnEmptyBatchAfterAnExactBoundary() throws Exception {
        var fake = new FakeEnumString(List.of("a", "b", "c", "d"));

        var actual = new EnumString(fake.comObject()).asCollection(2);

        assertEquals(List.of("a", "b", "c", "d"), List.copyOf(actual));
        assertEquals(List.of(2, 2, 2), fake.requests());
        assertEquals(List.of(2, 2, 0), fake.returnedCounts());
        assertEquals(1, fake.resetCount());
    }

    @Test
    void treeScanKeepsSameNamedDeepBranchesUnderDifferentParentsAndSkipsDuplicatePaths()
            throws Exception {
        Branch root = new Branch();
        Branch east = branch(root, "east");
        Branch west = branch(root, "west");
        Branch eastArea = branch(east, "area");
        Branch duplicateEastArea = branch(east, "area");
        Branch westArea = branch(west, "area");
        eastArea.setLeaves(List.of(new Leaf(eastArea, "temperature", "east-id")));
        duplicateEastArea.setLeaves(
                List.of(new Leaf(duplicateEastArea, "duplicate", "duplicate-id")));
        westArea.setLeaves(List.of(new Leaf(westArea, "temperature", "west-id")));
        east.setBranches(List.of(eastArea, duplicateEastArea));
        west.setBranches(List.of(westArea));
        root.setBranches(List.of(east, west));

        var scan =
                OpcDaBrowseSupport.scanTree(
                        root, new PrefilledTree(), new OpcDaBrowseSupport.TreeLimits(100, 100));

        assertEquals(List.of("east-id", "west-id"), List.copyOf(scan.itemIds()));
        assertEquals(5, scan.processedBranches());
        scan.requireComplete();
    }

    @Test
    void treeScanReportsLeafAndBranchFailuresAndBothLimits() throws Exception {
        Branch root = new Branch();
        Branch first = branch(root, "first");
        Branch second = branch(root, "second");
        root.setBranches(List.of(first, second));
        root.setLeaves(List.of(new Leaf(root, "one", "one"), new Leaf(root, "two", "two")));

        var failed =
                OpcDaBrowseSupport.scanTree(
                        root,
                        new OpcDaBrowseSupport.TreeAccess() {
                            @Override
                            public void fillLeaves(Branch branch) throws Exception {
                                if (branch == first) {
                                    throw new SocketTimeoutException("timed out");
                                }
                            }

                            @Override
                            public void fillBranches(Branch branch) throws Exception {
                                if (branch == second) {
                                    throw new SocketTimeoutException("branches timed out");
                                }
                            }
                        },
                        new OpcDaBrowseSupport.TreeLimits(100, 100));
        var idLimited =
                OpcDaBrowseSupport.scanTree(
                        root, new PrefilledTree(), new OpcDaBrowseSupport.TreeLimits(1, 100));
        var branchLimited =
                OpcDaBrowseSupport.scanTree(
                        root, new PrefilledTree(), new OpcDaBrowseSupport.TreeLimits(100, 1));

        assertEquals(
                List.of(
                        OpcDaBrowseSupport.TreeOperation.LEAVES,
                        OpcDaBrowseSupport.TreeOperation.BRANCHES),
                failed.failures().stream().map(OpcDaBrowseSupport.TreeFailure::operation).toList());
        assertThrows(IllegalStateException.class, failed::requireComplete);
        assertTrue(idLimited.itemLimitReached());
        assertThrows(IllegalStateException.class, idLimited::requireComplete);
        assertTrue(branchLimited.branchLimitReached());
        assertThrows(IllegalStateException.class, branchLimited::requireComplete);
    }

    @Test
    void flatBrowseFallsBackOnlyWhenUnavailable() throws Exception {
        var treeCalls = new AtomicInteger();
        var tree = completeScan("tree-id");

        var selected =
                OpcDaBrowseSupport.selectBrowse(
                        () -> null,
                        () -> {
                            treeCalls.incrementAndGet();
                            return tree;
                        });

        assertEquals(OpcDaBrowseSupport.BrowseMode.TREE, selected.mode());
        assertEquals(List.of("tree-id"), List.copyOf(selected.itemIds()));
        assertEquals(1, treeCalls.get());

        var timeout = new SocketTimeoutException("authentication transport timed out");
        assertEquals(
                timeout,
                assertThrows(
                        SocketTimeoutException.class,
                        () ->
                                OpcDaBrowseSupport.selectBrowse(
                                        () -> {
                                            throw timeout;
                                        },
                                        () -> {
                                            treeCalls.incrementAndGet();
                                            return tree;
                                        })));
        assertEquals(1, treeCalls.get());
    }

    @Test
    void browseAndAddValidationRejectsEmptyOrIncompleteResults() {
        assertThrows(
                IllegalStateException.class,
                () ->
                        new OpcDaBrowseSupport.BrowseSelection(
                                        OpcDaBrowseSupport.BrowseMode.FLAT, Set.of())
                                .requireItems());
        assertThrows(
                IllegalStateException.class,
                () -> OpcDaBrowseSupport.requireAllAdded(2, Map.of(), Map.of("a", 0xC0040007)));
        assertThrows(
                IllegalStateException.class,
                () -> OpcDaBrowseSupport.requireAllAdded(2, Map.of("a", new Object()), Map.of()));
    }

    @Test
    void addFailureRetriesOnlyAnEntirelyRejectedFlatResult() {
        assertTrue(
                OpcDaBrowseSupport.shouldRetryWithTree(
                        OpcDaBrowseSupport.BrowseMode.FLAT, 2, Map.of()));
        assertFalse(
                OpcDaBrowseSupport.shouldRetryWithTree(
                        OpcDaBrowseSupport.BrowseMode.FLAT, 2, Map.of("a", new Object())));
        assertFalse(
                OpcDaBrowseSupport.shouldRetryWithTree(
                        OpcDaBrowseSupport.BrowseMode.TREE, 2, Map.of()));
    }

    @Test
    void connectionInformationUsesSpacedProgIdWithoutAlsoSelectingClsid() {
        var connection =
                TestConnectionSettings.load(
                        Map.of(
                                "OPCDA_HOST", "10.0.0.1",
                                "OPCDA_PROG_ID", "Vendor OPC Server.1"));

        var information = OpcDaBrowseSupport.connectionInformation(connection);

        assertEquals("Vendor OPC Server.1", information.getProgId());
        assertNull(information.getClsid());
    }

    private OpcDaBrowseSupport.TreeScan completeScan(String id) {
        return new OpcDaBrowseSupport.TreeScan(List.of(id), 1, List.of(), false, false);
    }

    private Branch branch(Branch parent, String name) {
        return new Branch(parent, name);
    }

    private static final class PrefilledTree implements OpcDaBrowseSupport.TreeAccess {
        @Override
        public void fillLeaves(Branch branch) {}

        @Override
        public void fillBranches(Branch branch) {}
    }

    private static final class FakeEnumString implements InvocationHandler {
        // JICallBuilder reports interface opnums after IUnknown's three methods.
        private static final int NEXT_OPNUM = 3;
        private static final int RESET_OPNUM = 5;

        private final List<String> values;
        private final List<Integer> requests = new ArrayList<>();
        private final List<Integer> returnedCounts = new ArrayList<>();
        private final IJIComObject comObject;
        private int position;
        private int resetCount;

        FakeEnumString(List<String> values) {
            this.values = List.copyOf(values);
            this.comObject =
                    (IJIComObject)
                            Proxy.newProxyInstance(
                                    IJIComObject.class.getClassLoader(),
                                    new Class<?>[] {IJIComObject.class},
                                    this);
        }

        IJIComObject comObject() {
            return comObject;
        }

        List<Integer> requests() {
            return List.copyOf(requests);
        }

        List<Integer> returnedCounts() {
            return List.copyOf(returnedCounts);
        }

        int resetCount() {
            return resetCount;
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            return switch (method.getName()) {
                case "queryInterface" -> proxy;
                case "call" -> call((JICallBuilder) args[0]);
                case "toString" -> "FakeEnumString";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> throw new AssertionError("unexpected IJIComObject call: " + method);
            };
        }

        private Object[] call(JICallBuilder call) {
            return switch (call.getOpnum()) {
                case NEXT_OPNUM -> next((Integer) call.getInParamAt(0));
                case RESET_OPNUM -> reset();
                default -> throw new AssertionError("unexpected enum opnum: " + call.getOpnum());
            };
        }

        private Object[] next(int requested) {
            requests.add(requested);
            int count = Math.min(requested, values.size() - position);
            JIString[] batch = new JIString[count];
            for (int i = 0; i < count; i++) {
                batch[i] = new JIString(values.get(position + i));
            }
            position += count;
            returnedCounts.add(count);
            return new Object[] {new JIArray(batch), count};
        }

        private Object[] reset() {
            position = 0;
            resetCount++;
            return new Object[0];
        }
    }
}
