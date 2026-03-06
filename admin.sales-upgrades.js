(() => {
    if (window.__ABOUBA_SALES_UPGRADES_INIT__) return;
    window.__ABOUBA_SALES_UPGRADES_INIT__ = true;
    const BUILD_TAG = "sales-upgrades-v6-no-auto-loop";
    console.log(BUILD_TAG);

    const salesStatus = document.getElementById("salesStatus");
    const salesBody = document.getElementById("salesBody");
    const metricTotalRevenue = document.getElementById("metricTotalRevenue");
    const metricSoldItems = document.getElementById("metricSoldItems");
    const metricTodaySold = document.getElementById("metricTodaySold");
    const metricMonthlyRevenue = document.getElementById("metricMonthlyRevenue");
    const inventoryTitle = document.getElementById("inventoryTitle");
    const exportCsvBtn = document.getElementById("exportCsvBtn");
    const resetFiltersBtn = document.getElementById("resetFiltersBtn");
    const filterFrom = document.getElementById("filterFrom");
    const filterTo = document.getElementById("filterTo");
    const filterProduct = document.getElementById("filterProduct");
    const filterSoldBy = document.getElementById("filterSoldBy");

    let dailyRevenueChart = null;
    let topProductsChart = null;
    let refreshTimer = null;
    let currentAdminEmail = "";
    let isRefreshing = false;
    let needsRefresh = false;
    let lastRefreshAt = 0;
    let salesChannel = null;
    const seenRealtimeEvents = new Map();
    const inFlightSales = new Set();
    const inFlightDeletes = new Set();
    const inventoryResetClicks = [];

    function setStatusText(el, message, isError = false) {
        if (!el) return;
        if (typeof setStatus === "function") {
            setStatus(el, message, isError);
            return;
        }
        el.textContent = message;
        el.classList.toggle("error", isError);
    }

    function safeText(value) {
        if (typeof escapeHtml === "function") return escapeHtml(value);
        return String(value ?? "");
    }

    function rwf(value) {
        if (typeof formatRwf === "function") return formatRwf(value);
        return new Intl.NumberFormat("en-RW", { style: "currency", currency: "RWF", maximumFractionDigits: 0 }).format(Number(value) || 0);
    }

    function dateKey(dateLike) {
        const d = new Date(dateLike);
        if (Number.isNaN(d.getTime())) return "";
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, "0");
        const day = String(d.getDate()).padStart(2, "0");
        return `${y}-${m}-${day}`;
    }

    function monthKey(dateLike) {
        const d = new Date(dateLike);
        if (Number.isNaN(d.getTime())) return "";
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, "0");
        return `${y}-${m}`;
    }

    async function getSessionEmail() {
        const { data, error } = await supabaseClient.auth.getSession();
        if (error || !data?.session?.user?.email) return "";
        return data.session.user.email;
    }

    async function ensureSalesTable() {
        const { error } = await supabaseClient.rpc("create_sales_table_if_not_exists");
        if (!error) return;
        const message = error.message || "";
        if (/not found|does not exist/i.test(message)) {
            console.warn("Create this SQL function for automatic table setup: create_sales_table_if_not_exists()");
            return;
        }
        console.warn("Sales table ensure warning:", message);
    }

    async function backfillLegacySoldProductsIntoSales() {
        const { data: soldProducts, error: soldError } = await supabaseClient
            .from("products")
            .select("id,name,price,created_at,status")
            .eq("status", "sold");

        if (soldError) {
            console.warn("Legacy sold-products read warning:", soldError);
            return 0;
        }

        if (!soldProducts?.length) return 0;
        const soldIds = soldProducts.map((p) => p.id).filter(Boolean);
        if (!soldIds.length) return 0;

        const { data: existingSales, error: existingError } = await supabaseClient
            .from("sales")
            .select("product_id")
            .in("product_id", soldIds);

        if (existingError) {
            console.warn("Existing sales lookup warning:", existingError);
            return 0;
        }

        const existingSet = new Set((existingSales || []).map((row) => row.product_id).filter(Boolean));
        const rowsToInsert = soldProducts
            .filter((p) => !existingSet.has(p.id))
            .map((p) => {
                const price = Number(p.price) || 0;
                return {
                    product_id: p.id,
                    name: p.name || "Unnamed",
                    price_rwf: price,
                    quantity: 1,
                    total_price: price,
                    sold_date: p.created_at || new Date().toISOString(),
                    sold_by: "legacy-sync",
                    status: "completed"
                };
            });

        if (!rowsToInsert.length) return 0;
        const { error: insertError } = await supabaseClient.from("sales").insert(rowsToInsert);
        if (insertError) {
            console.warn("Legacy sales backfill warning:", insertError);
            return 0;
        }
        return rowsToInsert.length;
    }

    function renderSalesTable(rows) {
        renderSales(rows || []);
    }

    async function loadSalesReport() {
        const { data, error } = await supabaseClient
            .from("sales")
            .select("id, product_id, name, price_rwf, quantity, total_price, sold_date, sold_by, status")
            .order("sold_date", { ascending: false });

        if (error) {
            console.error(error);
            alert(error.message || "Failed to load sales report");
            return [];
        }

        renderSalesTable(data || []);
        return data || [];
    }

    async function updateDashboardMetrics() {
        const { data, error } = await supabaseClient
            .from("sales")
            .select("total_price, price_rwf, quantity, status");

        if (error) {
            console.error(error);
            alert(error.message || "Failed to load revenue metrics");
            return;
        }

        let revenue = 0;
        for (const sale of data || []) {
            const status = String(sale.status || "completed").toLowerCase();
            if (status === "returned" || status === "cancelled" || status === "void") continue;
            const qty = Math.max(1, Number(sale.quantity) || 1);
            const rowTotal = Number(sale.total_price);
            revenue += Number.isFinite(rowTotal) ? rowTotal : (Number(sale.price_rwf) || 0) * qty;
        }

        if (metricTotalRevenue) metricTotalRevenue.textContent = rwf(revenue);
        const totalRevenueEl = document.getElementById("totalRevenue");
        if (totalRevenueEl) totalRevenueEl.innerText = `${revenue} RWF`;
    }

    function calcMetrics(rows) {
        const today = dateKey(new Date());
        const month = monthKey(new Date());
        let totalRevenue = 0;
        let soldItems = 0;
        let todaySold = 0;
        let monthlyRevenue = 0;

        for (const row of rows) {
            const status = String(row.status || "completed").toLowerCase();
            if (status === "returned" || status === "cancelled" || status === "void") continue;
            const qty = Math.max(1, Number(row.quantity) || 1);
            const rowTotal = Number(row.total_price);
            const total = Number.isFinite(rowTotal) ? rowTotal : (Number(row.price_rwf) || 0) * qty;
            totalRevenue += total;
            soldItems += qty;
            if (dateKey(row.sold_date) === today) todaySold += qty;
            if (monthKey(row.sold_date) === month) monthlyRevenue += total;
        }

        return { totalRevenue, soldItems, todaySold, monthlyRevenue };
    }

    function renderMetrics(metrics) {
        if (!metricTotalRevenue) return;
        metricTotalRevenue.textContent = rwf(metrics.totalRevenue);
        metricSoldItems.textContent = String(metrics.soldItems);
        metricTodaySold.textContent = String(metrics.todaySold);
        metricMonthlyRevenue.textContent = rwf(metrics.monthlyRevenue);
    }

    function ensureCharts() {
        if (typeof Chart === "undefined") return;

        if (!dailyRevenueChart) {
            dailyRevenueChart = new Chart(document.getElementById("dailyRevenueChart"), {
                type: "line",
                data: { labels: [], datasets: [{ label: "Revenue", data: [], borderColor: "#1f8ef1", backgroundColor: "rgba(31,142,241,0.14)", fill: true, tension: 0.24 }] },
                options: { responsive: true, maintainAspectRatio: true, aspectRatio: 2, animation: false }
            });
        }

        if (!topProductsChart) {
            topProductsChart = new Chart(document.getElementById("topProductsChart"), {
                type: "bar",
                data: { labels: [], datasets: [{ label: "Qty Sold", data: [], backgroundColor: "#1b9c5a" }] },
                options: { responsive: true, maintainAspectRatio: true, aspectRatio: 2, animation: false }
            });
        }
    }

    function updateCharts(rows) {
        ensureCharts();
        if (!dailyRevenueChart || !topProductsChart) return;

        const byDay = new Map();
        const byProduct = new Map();
        for (const row of rows) {
            const status = String(row.status || "completed").toLowerCase();
            if (status === "returned" || status === "cancelled" || status === "void") continue;
            const day = dateKey(row.sold_date);
            const qty = Math.max(1, Number(row.quantity) || 1);
            const rowTotal = Number(row.total_price);
            const total = Number.isFinite(rowTotal) ? rowTotal : (Number(row.price_rwf) || 0) * qty;
            if (day) byDay.set(day, (byDay.get(day) || 0) + total);
            const name = row.name || "Unnamed";
            byProduct.set(name, (byProduct.get(name) || 0) + qty);
        }

        const labels = Array.from(byDay.keys()).sort();
        dailyRevenueChart.data.labels = labels;
        dailyRevenueChart.data.datasets[0].data = labels.map((k) => byDay.get(k));
        dailyRevenueChart.update();

        const top = Array.from(byProduct.entries()).sort((a, b) => b[1] - a[1]).slice(0, 5);
        topProductsChart.data.labels = top.map((x) => x[0]);
        topProductsChart.data.datasets[0].data = top.map((x) => x[1]);
        topProductsChart.update();
    }

    function renderSales(rows) {
        if (!salesBody) return;
        if (!rows.length) {
            salesBody.innerHTML = '<tr><td colspan="8">No sales yet.</td></tr>';
            return;
        }

        salesBody.innerHTML = rows.map((row) => {
            const status = String(row.status || "completed").toLowerCase();
            const isReturned = status === "returned";
            const soldDate = row.sold_date ? new Date(row.sold_date).toLocaleString() : "-";
            return `
                <tr>
                    <td>${safeText(soldDate)}</td>
                    <td>${safeText(row.name || "Unnamed")}</td>
                    <td>${safeText(String(Math.max(1, Number(row.quantity) || 1)))}</td>
                    <td>${rwf(row.price_rwf)}</td>
                    <td>${rwf(row.total_price)}</td>
                    <td>${safeText(row.sold_by || "-")}</td>
                    <td>${safeText(status)}</td>
                    <td><button class="btn btn-danger" ${isReturned ? "disabled" : ""} onclick="returnSale('${row.id}', '${row.product_id || ""}')">Return</button></td>
                </tr>
            `;
        }).join("");
    }

    async function refreshSalesDashboard(showLoading = true) {
        if (isRefreshing) {
            needsRefresh = true;
            return;
        }
        isRefreshing = true;
        if (showLoading) setStatusText(salesStatus, "Loading sales...");
        try {
            const rows = await loadSalesReport();
            renderMetrics(calcMetrics(rows));
            updateCharts(rows);
            await updateDashboardMetrics();
            setStatusText(salesStatus, `${rows.length} sale record(s).`);
        } catch (error) {
            console.error(error);
            setStatusText(salesStatus, error.message || "Failed to load sales.", true);
            if (salesBody) salesBody.innerHTML = '<tr><td colspan="8">Failed to load sales.</td></tr>';
        } finally {
            isRefreshing = false;
            if (needsRefresh) {
                needsRefresh = false;
                await refreshSalesDashboard(false);
            }
        }
    }

    function queueSalesRefresh() {
        const now = Date.now();
        if (now - lastRefreshAt < 1000) return;
        lastRefreshAt = now;
        if (refreshTimer) clearTimeout(refreshTimer);
        refreshTimer = setTimeout(() => {
            refreshTimer = null;
            refreshSalesDashboard(false);
        }, 250);
    }

    function handleSalesRealtime(payload) {
        const rowId = payload?.new?.id || payload?.old?.id || "";
        const eventType = payload?.eventType || "";
        const commitTs = payload?.commit_timestamp || "";
        const key = `${eventType}:${rowId}:${commitTs}`;

        const now = Date.now();
        for (const [k, ts] of seenRealtimeEvents.entries()) {
            if (now - ts > 60000) seenRealtimeEvents.delete(k);
        }

        if (seenRealtimeEvents.has(key)) return;
        seenRealtimeEvents.set(key, now);
        if (payload?.eventType !== "INSERT") return;
        queueSalesRefresh();
    }

    function csvCell(value) {
        return `"${String(value ?? "").replace(/"/g, "\"\"")}"`;
    }

    function triggerCsvDownload(fileName, content) {
        const blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = fileName;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
    }

    async function exportSalesCsv() {
        if (!exportCsvBtn) return;
        const original = exportCsvBtn.textContent;
        exportCsvBtn.disabled = true;
        exportCsvBtn.textContent = "Exporting...";
        try {
            let query = supabaseClient
                .from("sales")
                .select("id,product_id,name,price_rwf,quantity,total_price,sold_date,sold_by,status")
                .order("sold_date", { ascending: false });

            if (filterFrom?.value) query = query.gte("sold_date", `${filterFrom.value}T00:00:00`);
            if (filterTo?.value) query = query.lte("sold_date", `${filterTo.value}T23:59:59`);
            if (filterProduct?.value?.trim()) query = query.ilike("name", `%${filterProduct.value.trim()}%`);
            if (filterSoldBy?.value?.trim()) query = query.ilike("sold_by", `%${filterSoldBy.value.trim()}%`);

            const { data, error } = await query;
            if (error) throw new Error(error.message || "CSV export failed");

            const headers = ["id", "product_id", "name", "price_rwf", "quantity", "total_price", "sold_date", "sold_by", "status"];
            const lines = [headers.join(",")];
            for (const row of data || []) {
                lines.push(headers.map((h) => csvCell(row[h])).join(","));
            }
            triggerCsvDownload(`sales-report-${new Date().toISOString().slice(0, 10)}.csv`, `${lines.join("\n")}\n`);
        } catch (error) {
            console.error(error);
            alert(error.message || "CSV export failed");
        } finally {
            exportCsvBtn.disabled = false;
            exportCsvBtn.textContent = original;
        }
    }

    async function resetSalesFilters() {
        if (filterFrom) filterFrom.value = "";
        if (filterTo) filterTo.value = "";
        if (filterProduct) filterProduct.value = "";
        if (filterSoldBy) filterSoldBy.value = "";
        await refreshSalesDashboard(false);
    }

    async function resetAdminDatabaseData() {
        const { error: rpcError } = await supabaseClient.rpc("reset_admin_dashboard_data");
        if (!rpcError) return;
        throw new Error(formatDbError("Database reset failed", rpcError));
    }

    async function resetAdminDashboard() {
        if (resetFiltersBtn) {
            resetFiltersBtn.disabled = true;
            resetFiltersBtn.textContent = "Resetting...";
        }
        try {
            await exportSalesCsv();
            await resetAdminDatabaseData();
            await resetSalesFilters();
            await Promise.all([loadInventory(), refreshSalesDashboard(false)]);
            alert("Dashboard reset complete. Report downloaded, sales cleared, and inventory cleared.");
        } catch (error) {
            console.error(error);
            alert(error.message || "Dashboard reset failed");
        } finally {
            if (resetFiltersBtn) {
                resetFiltersBtn.disabled = false;
                resetFiltersBtn.textContent = "Reset Filters";
            }
        }
    }

    async function confirmAndResetSalesFilters() {
        const confirmed = window.confirm(
            "This will download the current full sales report CSV, then reset database data (clear sales and clear inventory), and reload the dashboard. Continue?"
        );
        if (!confirmed) return;
        const finalCheck = window.prompt('Type RESET to confirm destructive reset:');
        if (finalCheck !== "RESET") {
            alert("Reset canceled. Confirmation text did not match.");
            return;
        }
        await resetAdminDashboard();
    }

    async function handleInventoryResetSequence() {
        const now = Date.now();
        inventoryResetClicks.push(now);
        while (inventoryResetClicks.length && now - inventoryResetClicks[0] > 4000) {
            inventoryResetClicks.shift();
        }
        if (inventoryResetClicks.length >= 5) {
            inventoryResetClicks.length = 0;
            await resetSalesFilters();
        }
    }

    function formatDbError(prefix, err) {
        if (!err) return prefix;
        const parts = [
            err.message || "",
            err.details || "",
            err.hint || "",
            err.code ? `code=${err.code}` : "",
            err.status ? `status=${err.status}` : ""
        ].filter(Boolean);
        return `${prefix}: ${parts.join(" | ")}`;
    }

    window.markSold = async function(productId, buttonEl) {
        if (inFlightSales.has(productId)) return;
        inFlightSales.add(productId);
        if (buttonEl) buttonEl.disabled = true;

        try {
            if (!currentAdminEmail) currentAdminEmail = await getSessionEmail();
            const soldBy = currentAdminEmail || "admin";

            const { data: product, error: fetchError } = await supabaseClient
                .from("products")
                .select("*")
                .eq("id", productId)
                .single();

            if (fetchError) {
                console.error(fetchError);
                alert(fetchError.message || "Failed to load product");
                return;
            }

            if (!product) {
                alert("Product not found");
                return;
            }

            const rawStock = Number(product.stock);
            const currentStock = Number.isFinite(rawStock) ? rawStock : 1;
            if (product.status === "sold" || currentStock <= 0) {
                alert("Product already sold");
                return;
            }

            const price = Number(product.price) || 0;
            const { data: insertedSale, error: salesError } = await supabaseClient
                .from("sales")
                .insert([{
                    product_id: product.id,
                    name: product.name || "Unnamed",
                    price_rwf: price,
                    quantity: 1,
                    total_price: price,
                    sold_by: soldBy,
                    status: "completed"
                }])
                .select("id")
                .single();

            if (salesError) {
                console.error(salesError);
                alert(salesError.message || "Failed to record sale");
                return;
            }

            const nextStock = Math.max(0, currentStock - 1);
            const nextStatus = nextStock > 0 ? "available" : "sold";
            const { error: updateError } = await supabaseClient
                .from("products")
                .update({ status: nextStatus, stock: nextStock })
                .eq("id", productId);

            if (updateError) {
                console.error(updateError);
                alert(updateError.message || "Failed to update inventory");
                if (insertedSale?.id) {
                    const { error: rollbackError } = await supabaseClient
                        .from("sales")
                        .delete()
                        .eq("id", insertedSale.id);
                    if (rollbackError) {
                        console.error(rollbackError);
                        alert(rollbackError.message || "Failed to rollback partial sale");
                    }
                }
                return;
            }

            alert("Sale recorded successfully");
            await Promise.all([loadInventory(), refreshSalesDashboard(false)]);
        } finally {
            inFlightSales.delete(productId);
            if (buttonEl) buttonEl.disabled = false;
        }
    };

    window.returnSale = async function(saleId, productId) {
        try {
            const { data: sale, error: saleError } = await supabaseClient
                .from("sales")
                .select("id,quantity,status")
                .eq("id", saleId)
                .single();
            if (saleError || !sale) {
                console.error(saleError);
                throw new Error(saleError?.message || "Sale not found");
            }
            if (sale.status === "returned") return;

            const { data: product, error: productError } = await supabaseClient
                .from("products")
                .select("id,stock")
                .eq("id", productId)
                .single();
            if (productError || !product) {
                console.error(productError);
                throw new Error(productError?.message || "Product not found");
            }

            const qty = Math.max(1, Number(sale.quantity) || 1);
            const nextStock = (Number(product.stock) || 0) + qty;

            const { error: rpcError } = await supabaseClient.rpc("return_sale_and_restock", {
                p_sale_id: sale.id,
                p_product_id: product.id,
                p_restock_qty: qty,
                p_next_stock: nextStock
            });

            if (rpcError) {
                console.error(rpcError);
                const { error: statusError } = await supabaseClient
                    .from("sales")
                    .update({ status: "returned" })
                    .eq("id", sale.id)
                    .eq("status", "completed");
                if (statusError) {
                    console.error(statusError);
                    throw new Error(statusError.message || "Failed to mark return");
                }

                const { error: restockError } = await supabaseClient
                    .from("products")
                    .update({ stock: nextStock, status: "available" })
                    .eq("id", product.id);
                if (restockError) {
                    console.error(restockError);
                    throw new Error(restockError.message || "Failed to restock product");
                }
            }

            await Promise.all([loadInventory(), refreshSalesDashboard()]);
        } catch (error) {
            console.error(error);
            alert(`Return failed: ${error.message || "Unknown error"}`);
        }
    };

    window.deleteProduct = async function(productId, imageUrl, buttonEl) {
        if (inFlightDeletes.has(productId)) return;
        const confirmed = window.confirm("Delete this product permanently?");
        if (!confirmed) return;
        inFlightDeletes.add(productId);
        if (buttonEl) buttonEl.disabled = true;

        try {
            const { error: deleteError } = await supabaseClient
                .from("products")
                .delete()
                .eq("id", productId);

            if (!deleteError) {
                try {
                    const marker = `/storage/v1/object/public/${STORAGE_BUCKET}/`;
                    const index = (imageUrl || "").indexOf(marker);
                    if (index !== -1) {
                        const path = imageUrl.slice(index + marker.length).split("?")[0];
                        if (path) {
                            const { error: storageError } = await supabaseClient.storage.from(STORAGE_BUCKET).remove([path]);
                            if (storageError) {
                                console.error(storageError);
                                alert(storageError.message);
                            }
                        }
                    }
                } catch (cleanupErr) {
                    console.warn("Image cleanup warning:", cleanupErr);
                }

                await loadInventory();
                return;
            }

            console.error(deleteError);
            const errorText = `${deleteError.message || ""} ${deleteError.details || ""}`.toLowerCase();
            const isFkConflict = deleteError.code === "23503" ||
                deleteError.status === 409 ||
                errorText.includes("foreign key constraint") ||
                errorText.includes("violates");

            // Product is referenced by sales history; preserve history and archive product instead.
            if (isFkConflict) {
                const archiveConfirmed = window.confirm(
                    "This product has sales history and cannot be deleted. Archive it instead?"
                );
                if (!archiveConfirmed) return;

                const { error: archiveError } = await supabaseClient
                    .from("products")
                    .update({ status: "archived", stock: 0 })
                    .eq("id", productId);

                if (archiveError) {
                    console.error(archiveError);
                    alert(archiveError.message || "Archive failed");
                    return;
                }

                await loadInventory();
                await refreshSalesDashboard(false);
                return;
            }

            alert(deleteError.message || "Delete failed");
        } finally {
            inFlightDeletes.delete(productId);
            if (buttonEl) buttonEl.disabled = false;
        }
    };

    function subscribeSalesRealtime() {
        if (salesChannel) {
            try { supabaseClient.removeChannel(salesChannel); } catch (_) {}
        }
        salesChannel = supabaseClient
            .channel("public:sales:admin-enhanced")
            .on("postgres_changes", { event: "INSERT", schema: "public", table: "sales" }, handleSalesRealtime)
            .subscribe();
    }

    async function startEnhancements() {
        if (!supabaseClient || !salesStatus) return;
        currentAdminEmail = await getSessionEmail();
        await ensureSalesTable();
        const backfilledCount = await backfillLegacySoldProductsIntoSales();
        if (backfilledCount > 0) {
            setStatusText(salesStatus, `Synced ${backfilledCount} legacy sold product(s) to sales history.`);
        }
        await refreshSalesDashboard();
        // Realtime sales channel disabled to avoid loop storms in this deployment.
        // Polling disabled as well; dashboard refreshes on page load and after actions.
        // subscribeSalesRealtime();
        if (exportCsvBtn) exportCsvBtn.addEventListener("click", exportSalesCsv);
        if (resetFiltersBtn) resetFiltersBtn.addEventListener("click", confirmAndResetSalesFilters);
        if (inventoryTitle) inventoryTitle.addEventListener("click", handleInventoryResetSequence);
    }

    startEnhancements();
})();
