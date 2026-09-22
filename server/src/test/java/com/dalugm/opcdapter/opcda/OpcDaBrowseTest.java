/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.jinterop.dcom.common.JIException;
import org.jinterop.dcom.core.JIVariant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openscada.opc.dcom.common.impl.EnumString;
import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.AddFailedException;
import org.openscada.opc.lib.da.Group;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.ItemState;
import org.openscada.opc.lib.da.Server;
import org.openscada.opc.lib.da.browser.Branch;
import org.openscada.opc.lib.da.browser.FlatBrowser;
import org.openscada.opc.lib.da.browser.TreeBrowser;

/**
 * Standalone OPC DA address-space browser and item-type scanner. Configure it with the OPCDA_*
 * environment variables used by the other live tests and run `just opcda-browse`. Prints the server
 * clock, a VARIANT type distribution over all browsable items, and every VT_DATE (datetime) item
 * with its current value.
 */
@EnabledIfSystemProperty(named = "opcda.it.enabled", matches = "true")
class OpcDaBrowseTest {

    private static final boolean PRINT_ALL_ITEMS = false;
    private static final boolean DEVICE_READ = false;
    private static final int ITEM_CHUNK_SIZE = 500;
    private static final int BROWSE_BATCH_SIZE = EnumString.DEFAULT_BATCH_SIZE;
    private static final int MAX_TREE_IDS = 200_000;
    private static final int MAX_TREE_BRANCHES = 100_000;

    @Test
    void browseAllItems() throws Exception {
        var connection = TestConnectionSettings.load();
        ConnectionInformation info = OpcDaBrowseSupport.connectionInformation(connection);

        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(
                        4, Thread.ofPlatform().daemon().name("browse-exec-", 0).factory());
        Server server = new Server(info, executor);
        try {
            server.connect();
            System.out.printf("connected to %s, browsing...%n", connection.getHost());

            OpcDaBrowseSupport.BrowseSelection selection =
                    OpcDaBrowseSupport.selectBrowse(
                                    () -> flatBrowse(server), () -> collectTreeIds(server))
                            .requireItems();
            Set<String> items = new TreeSet<>(selection.itemIds());
            System.out.printf(
                    "browsed via %s, first 3 ids: %s%n",
                    selection.mode().name().toLowerCase(), items.stream().limit(3).toList());
            if (PRINT_ALL_ITEMS) {
                items.forEach(System.out::println);
            }
            System.out.printf("--- total: %d item(s) ---%n", items.size());

            printServerClock(server);
            scanItemTypes(server, selection.mode(), items);
        } finally {
            server.dispose();
            executor.shutdown();
        }
    }

    private void printServerClock(Server server) throws Exception {
        var status = server.getServerState();
        if (status == null || status.getCurrentTime() == null) {
            System.out.println("server clock: unavailable");
            return;
        }
        long serverTimeMs = status.getCurrentTime().asCalendar().getTimeInMillis();
        System.out.printf(
                "server clock: %s (diff from local: %dms)%n",
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(serverTimeMs)),
                serverTimeMs - System.currentTimeMillis());
    }

    private void scanItemTypes(
            Server server, OpcDaBrowseSupport.BrowseMode browseMode, Set<String> ids)
            throws Exception {
        Group group = server.addGroup("type-scan");
        Map<String, Integer> addErrors = new LinkedHashMap<>();
        Map<String, Item> added = addToGroup(group, new ArrayList<>(ids), addErrors);
        printAddSummary(added, ids.size(), addErrors);

        if (OpcDaBrowseSupport.shouldRetryWithTree(browseMode, ids.size(), added)) {
            System.out.println("all flat-browsed ids rejected; running controlled tree scan...");
            Set<String> treeIds = new TreeSet<>(collectTreeIds(server).requireComplete().itemIds());
            System.out.printf("tree scan collected %d leaf id(s)%n", treeIds.size());
            new OpcDaBrowseSupport.BrowseSelection(OpcDaBrowseSupport.BrowseMode.TREE, treeIds)
                    .requireItems();
            addErrors.clear();
            added = addToGroup(group, new ArrayList<>(treeIds), addErrors);
            printAddSummary(added, treeIds.size(), addErrors);
            ids = treeIds;
        }
        OpcDaBrowseSupport.requireAllAdded(ids.size(), added, addErrors);

        Map<Integer, Integer> typeCounts = new TreeMap<>();
        List<Map.Entry<String, Date>> dateItems = new ArrayList<>();
        List<Map.Entry<String, Item>> entries = new ArrayList<>(added.entrySet());
        for (int i = 0; i < entries.size(); i += ITEM_CHUNK_SIZE) {
            List<Map.Entry<String, Item>> chunk =
                    entries.subList(i, Math.min(i + ITEM_CHUNK_SIZE, entries.size()));
            Item[] chunkItems = chunk.stream().map(Map.Entry::getValue).toArray(Item[]::new);
            Map<Item, ItemState> states = group.read(DEVICE_READ, chunkItems);
            for (var entry : chunk) {
                ItemState st = states.get(entry.getValue());
                int type = variantType(st);
                typeCounts.merge(type, 1, Integer::sum);
                if (type == JIVariant.VT_DATE && st != null && st.getValue() != null) {
                    dateItems.add(Map.entry(entry.getKey(), st.getValue().getObjectAsDate()));
                }
            }
        }

        System.out.println("--- item type distribution ---");
        typeCounts.forEach((type, count) -> System.out.printf("%-12s %d%n", typeName(type), count));
        System.out.printf("--- %d datetime (VT_DATE) item(s) ---%n", dateItems.size());
        for (var entry : dateItems) {
            System.out.printf("%s = %s%n", entry.getKey(), entry.getValue());
        }
    }

    private OpcDaBrowseSupport.TreeScan collectTreeIds(Server server) throws Exception {
        quietJulSpam();
        TreeBrowser tb = server.getTreeBrowser();
        if (tb == null) {
            throw new IllegalStateException("server does not expose a hierarchical browser");
        }
        tb.setBatchSize(BROWSE_BATCH_SIZE);
        return OpcDaBrowseSupport.scanTree(
                new Branch(),
                new OpcDaBrowseSupport.TreeAccess() {
                    @Override
                    public void fillLeaves(Branch branch) throws Exception {
                        tb.fillLeaves(branch);
                    }

                    @Override
                    public void fillBranches(Branch branch) throws Exception {
                        tb.fillBranches(branch);
                    }
                },
                new OpcDaBrowseSupport.TreeLimits(MAX_TREE_IDS, MAX_TREE_BRANCHES));
    }

    private List<String> flatBrowse(Server server) throws Exception {
        FlatBrowser browser = server.getFlatBrowser();
        if (browser == null) {
            System.out.println("flat browser unavailable; trying tree browse...");
            return null;
        }
        browser.setBatchSize(BROWSE_BATCH_SIZE);
        return List.copyOf(browser.browse());
    }

    private void quietJulSpam() {
        java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.WARNING);
        java.util.logging.Logger.getLogger("rpc").setLevel(java.util.logging.Level.WARNING);
        java.util.logging.Logger.getLogger("org.jinterop")
                .setLevel(java.util.logging.Level.WARNING);
    }

    private Map<String, Item> addToGroup(
            Group group, List<String> idList, Map<String, Integer> addErrors) throws Exception {
        Map<String, Item> added = new LinkedHashMap<>();
        for (int i = 0; i < idList.size(); i += ITEM_CHUNK_SIZE) {
            List<String> chunk = idList.subList(i, Math.min(i + ITEM_CHUNK_SIZE, idList.size()));
            try {
                added.putAll(group.addItems(chunk.toArray(String[]::new)));
            } catch (AddFailedException e) {
                added.putAll(e.getItems());
                addErrors.putAll(e.getErrors());
            }
        }
        return added;
    }

    private void printAddSummary(
            Map<String, Item> added, int total, Map<String, Integer> addErrors) {
        System.out.printf("added %d/%d item(s) to group%n", added.size(), total);
        if (!addErrors.isEmpty()) {
            System.out.printf("--- %d add failure(s), first 10:%n", addErrors.size());
            addErrors.entrySet().stream()
                    .limit(10)
                    .forEach(
                            en ->
                                    System.out.printf(
                                            "%s -> HRESULT 0x%08X%n",
                                            en.getKey(), en.getValue() & 0xFFFFFFFFL));
        }
    }

    private int variantType(ItemState st) throws JIException {
        if (st == null || st.getValue() == null) {
            return -1;
        }
        return st.getValue().getType() & ~JIVariant.VT_BYREF;
    }

    private String typeName(int type) {
        return switch (type) {
            case -1 -> "NO_VALUE";
            case JIVariant.VT_EMPTY -> "VT_EMPTY";
            case JIVariant.VT_NULL -> "VT_NULL";
            case JIVariant.VT_I2 -> "VT_I2";
            case JIVariant.VT_I4 -> "VT_I4";
            case JIVariant.VT_R4 -> "VT_R4";
            case JIVariant.VT_R8 -> "VT_R8";
            case JIVariant.VT_BOOL -> "VT_BOOL";
            case JIVariant.VT_DATE -> "VT_DATE";
            case JIVariant.VT_BSTR -> "VT_BSTR";
            case JIVariant.VT_CY -> "VT_CY";
            case JIVariant.VT_UI1 -> "VT_UI1";
            default -> "0x" + Integer.toHexString(type);
        };
    }
}
