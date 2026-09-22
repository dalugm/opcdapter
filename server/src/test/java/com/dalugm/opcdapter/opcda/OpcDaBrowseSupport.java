/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.browser.Branch;
import org.openscada.opc.lib.da.browser.Leaf;

import com.dalugm.opcdapter.api.opcda.v1.Connection;

final class OpcDaBrowseSupport {

    enum BrowseMode {
        FLAT,
        TREE
    }

    enum TreeOperation {
        LEAVES,
        BRANCHES
    }

    @FunctionalInterface
    interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    interface TreeAccess {
        void fillLeaves(Branch branch) throws Exception;

        void fillBranches(Branch branch) throws Exception;
    }

    record TreeLimits(int maxItemIds, int maxBranches) {
        TreeLimits {
            if (maxItemIds <= 0 || maxBranches <= 0) {
                throw new IllegalArgumentException("tree browse limits must be positive");
            }
        }
    }

    record TreeFailure(List<String> branchPath, TreeOperation operation, Exception cause) {}

    record TreeScan(
            Collection<String> itemIds,
            int processedBranches,
            List<TreeFailure> failures,
            boolean itemLimitReached,
            boolean branchLimitReached) {
        TreeScan {
            itemIds = Collections.unmodifiableSet(new TreeSet<>(itemIds));
            failures = List.copyOf(failures);
        }

        TreeScan requireComplete() {
            if (!failures.isEmpty() || itemLimitReached || branchLimitReached) {
                String firstFailure =
                        failures.isEmpty()
                                ? ""
                                : ", first failure="
                                        + failures.getFirst().operation()
                                        + " at "
                                        + failures.getFirst().branchPath()
                                        + ": "
                                        + failures.getFirst().cause().getMessage();
                throw new IllegalStateException(
                        "tree browse incomplete: "
                                + failures.size()
                                + " branch operation failure(s), item limit reached="
                                + itemLimitReached
                                + ", branch limit reached="
                                + branchLimitReached
                                + firstFailure);
            }
            return this;
        }
    }

    record BrowseSelection(BrowseMode mode, Collection<String> itemIds) {
        BrowseSelection {
            itemIds = Collections.unmodifiableSet(new TreeSet<>(itemIds));
        }

        BrowseSelection requireItems() {
            if (itemIds.isEmpty()) {
                throw new IllegalStateException("browse returned no item IDs");
            }
            return this;
        }
    }

    private OpcDaBrowseSupport() {}

    static BrowseSelection selectBrowse(
            CheckedSupplier<? extends Collection<String>> flatBrowse,
            CheckedSupplier<TreeScan> treeBrowse)
            throws Exception {
        Collection<String> flatItems = flatBrowse.get();
        if (flatItems != null) {
            return new BrowseSelection(BrowseMode.FLAT, flatItems);
        }
        TreeScan tree = treeBrowse.get().requireComplete();
        return new BrowseSelection(BrowseMode.TREE, tree.itemIds());
    }

    static TreeScan scanTree(Branch root, TreeAccess browser, TreeLimits limits) {
        Set<String> ids = new TreeSet<>();
        Set<List<String>> visited = new HashSet<>();
        List<TreeFailure> failures = new ArrayList<>();
        Deque<Branch> queue = new ArrayDeque<>();
        enqueueBranch(queue, visited, root);
        int processed = 0;
        boolean itemLimitReached = false;

        while (!queue.isEmpty() && processed < limits.maxBranches() && !itemLimitReached) {
            Branch branch = queue.remove();
            try {
                browser.fillLeaves(branch);
                for (Leaf leaf : branch.getLeaves()) {
                    String itemId = leaf.getItemId();
                    if (itemId == null || itemId.isBlank()) {
                        failures.add(
                                new TreeFailure(
                                        branchPath(branch),
                                        TreeOperation.LEAVES,
                                        new IllegalStateException("leaf has no item ID")));
                    } else if (!ids.contains(itemId) && ids.size() >= limits.maxItemIds()) {
                        itemLimitReached = true;
                        break;
                    } else {
                        ids.add(itemId);
                    }
                }
            } catch (Exception e) {
                failures.add(new TreeFailure(branchPath(branch), TreeOperation.LEAVES, e));
            }

            if (!itemLimitReached) {
                try {
                    browser.fillBranches(branch);
                    for (Branch child : branch.getBranches()) {
                        enqueueBranch(queue, visited, child);
                    }
                } catch (Exception e) {
                    failures.add(new TreeFailure(branchPath(branch), TreeOperation.BRANCHES, e));
                }
            }
            processed++;
        }

        return new TreeScan(ids, processed, failures, itemLimitReached, !queue.isEmpty());
    }

    static boolean shouldRetryWithTree(BrowseMode mode, int requested, Map<String, ?> added) {
        return mode == BrowseMode.FLAT && requested > 0 && added.isEmpty();
    }

    static void requireAllAdded(int requested, Map<String, ?> added, Map<String, Integer> errors) {
        if (!errors.isEmpty() || added.size() != requested) {
            throw new IllegalStateException(
                    "AddItems incomplete: added "
                            + added.size()
                            + "/"
                            + requested
                            + " item(s), "
                            + errors.size()
                            + " explicit failure(s)");
        }
    }

    static ConnectionInformation connectionInformation(Connection connection) {
        var information = new ConnectionInformation();
        information.setHost(connection.getHost());
        information.setDomain(connection.getDomain());
        information.setUser(connection.getUsername());
        information.setPassword(connection.getCredentials().getPassword());
        if (connection.hasClsId()) {
            information.setClsid(connection.getClsId());
        } else if (connection.hasProgId()) {
            information.setProgId(connection.getProgId());
        } else {
            throw new IllegalArgumentException("connection has no CLSID or ProgID");
        }
        return information;
    }

    private static void enqueueBranch(
            Deque<Branch> queue, Set<List<String>> visited, Branch branch) {
        if (visited.add(branchPath(branch))) {
            queue.add(branch);
        }
    }

    private static List<String> branchPath(Branch branch) {
        return List.copyOf(branch.getBranchStack());
    }
}
