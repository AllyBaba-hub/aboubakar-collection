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
    const exportCsvBtn = document.getElementById("exportCsvBtn");
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

    async function fetchSales() {
        const { data, error } = await supabaseClient
            .from("sales")
            .select("id, product_id, name, price_rwf, quantity, total_price, sold_date, sold_by, status")
            .order("sold_date", { ascending: false })
            .limit(500);
        if (error) throw new Error(error.message || "Failed to load sales");
        return data || [];
    }

    function calcMetrics(rows) {
        const today = dateKey(new Date());
        const month = monthKey(new Date());
        let totalRevenue = 0;
        let soldItems = 0;
        let todaySold = 0;
        let monthlyRevenue = 0;

        for (const row of rows) {
            if (row.status !== "completed") continue;
            const qty = Number(row.quantity) || 0;
            const total = Number(row.total_price) || 0;
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
            if (row.status !== "completed") continue;
            const day = dateKey(row.sold_date);
            if (day) byDay.set(day, (byDay.get(day) || 0) + (Number(row.total_price) || 0));
            const name = row.name || "Unnamed";
            byProduct.set(name, (byProduct.get(name) || 0) + (Number(row.quantity) || 0));
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
            const isReturned = row.status === "returned";
            const soldDate = row.sold_date ? new Date(row.sold_date).toLocaleString() : "-";
            return `
                <tr>
                    <td>${safeText(soldDate)}</td>
                    <td>${safeText(row.name || "Unnamed")}</td>
                    <td>${safeText(String(row.quantity || 0))}</td>
                    <td>${rwf(row.price_rwf)}</td>
                    <td>${rwf(row.total_price)}</td>
                    <td>${safeText(row.sold_by || "-")}</td>
                    <td>${safeText(row.status || "completed")}</td>
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
            const rows = await fetchSales();
            renderSales(rows);
            renderMetrics(calcMetrics(rows));
            updateCharts(rows);
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
            alert(error.message || "CSV export failed");
        } finally {
            exportCsvBtn.disabled = false;
            exportCsvBtn.textContent = original;
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

            const { error } = await supabaseClient
                .from("products")
                .update({ status: "sold" })
                .eq("id", productId);

            if (error) {
                console.error(error);
                alert(error.message || "Failed to mark product as sold");
                return;
            }

            alert("Product marked as sold");
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
            if (saleError || !sale) throw new Error(saleError?.message || "Sale not found");
            if (sale.status === "returned") return;

            const { data: product, error: productError } = await supabaseClient
                .from("products")
                .select("id,stock")
                .eq("id", productId)
                .single();
            if (productError || !product) throw new Error(productError?.message || "Product not found");

            const qty = Math.max(1, Number(sale.quantity) || 1);
            const nextStock = (Number(product.stock) || 0) + qty;

            const { error: rpcError } = await supabaseClient.rpc("return_sale_and_restock", {
                p_sale_id: sale.id,
                p_product_id: product.id,
                p_restock_qty: qty,
                p_next_stock: nextStock
            });

            if (rpcError) {
                const { error: statusError } = await supabaseClient
                    .from("sales")
                    .update({ status: "returned" })
                    .eq("id", sale.id)
                    .eq("status", "completed");
                if (statusError) throw new Error(statusError.message || "Failed to mark return");

                const { error: restockError } = await supabaseClient
                    .from("products")
                    .update({ stock: nextStock, status: "available" })
                    .eq("id", product.id);
                if (restockError) throw new Error(restockError.message || "Failed to restock product");
            }

            await Promise.all([loadInventory(), refreshSalesDashboard()]);
        } catch (error) {
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
        await refreshSalesDashboard();
        // Realtime sales channel disabled to avoid loop storms in this deployment.
        // Polling disabled as well; dashboard refreshes on page load and after actions.
        // subscribeSalesRealtime();
        if (exportCsvBtn) exportCsvBtn.addEventListener("click", exportSalesCsv);
    }

    startEnhancements();
})();
