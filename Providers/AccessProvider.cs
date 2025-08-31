using System.Data;
using System.Data.Odbc;
using WebApplication11.Interfaces;
using static WebApplication11.Models.Models;

namespace WebApplication11.Providers
{
    public class AccessProvider : IDataProvider
    {
        private readonly string _connStr = @"Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=C:\Users\Patrick\MobileSoftwareDevelopment\6.Semester\Bachelorarbeit\Database\NorthwindTest.accdb;Uid=Admin;Pwd=;";

        private OdbcConnection Open()
        {
            var c = new OdbcConnection(_connStr);
            c.Open();
            return c;
        }

        private static T GetVal<T>(IDataRecord r, string name)
        {
            var v = r[name];
            return v == DBNull.Value ? default! : (T)Convert.ChangeType(v, typeof(T));
        }
        private static T? GetValOrNull<T>(IDataRecord r, string name) where T : struct
        {
            var v = r[name];
            return v == DBNull.Value ? (T?)null : (T?)Convert.ChangeType(v, typeof(T));
        }

        public async Task<OrderDto?> GetOrderWithLinesAsync(int orderId, CancellationToken ct = default)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                SELECT
                    o.[Order ID]        AS OrderID,
                    o.[Customer ID]     AS CustomerID,
                    o.[Employee ID]     AS EmployeeID,
                    o.[Order Date]      AS OrderDate,
                    o.[Shipper ID]      AS ShipperID,
                    o.[Shipping Fee]    AS ShippingFee,
                    od.[Product ID]     AS ProductID,
                    od.[Unit Price]     AS UnitPrice,
                    od.[Quantity]       AS Quantity,
                    od.[Discount]       AS Discount,
                    p.[Product Name]    AS ProductName
                FROM (Orders AS o
                      INNER JOIN [Order Details] AS od ON o.[Order ID] = od.[Order ID])
                INNER JOIN Products AS p ON p.[ID] = od.[Product ID]
                WHERE o.[Order ID] = ?";
            cmd.Parameters.Add("?", OdbcType.Int).Value = orderId;

            using var rd = await cmd.ExecuteReaderAsync(ct);
            if (!rd.HasRows) return null;

            await rd.ReadAsync(ct);

            var orderID = GetVal<int>(rd, "OrderID");
            var customerID = GetVal<int>(rd, "CustomerID");
            var employeeID = GetVal<int>(rd, "EmployeeID");
            var orderDate = GetVal<DateTime>(rd, "OrderDate");
            var shipperID = GetVal<int>(rd, "ShipperID");
            var shippingFee = GetVal<decimal>(rd, "ShippingFee");

            var lines = new List<OrderLineDto> {
                new(
                    GetVal<int>(rd, "ProductID"),
                    GetVal<decimal>(rd, "UnitPrice"),
                    GetVal<double>(rd, "Quantity"),
                    GetVal<double>(rd, "Discount"),
                    GetVal<string>(rd, "ProductName"))
            };
            while (await rd.ReadAsync(ct))
            {
                lines.Add(new OrderLineDto(
                    GetVal<int>(rd, "ProductID"),
                    GetVal<decimal>(rd, "UnitPrice"),
                    GetVal<double>(rd, "Quantity"),
                    GetVal<double>(rd, "Discount"),
                    GetVal<string>(rd, "ProductName")));
            }

            return new OrderDto(orderID, customerID, employeeID, orderDate, shipperID, shippingFee, lines);
        }

        public async Task<List<OrderHistoryRow>> GetCustomerOrderHistoryAsync(
            int customerId, int pageSize, CancellationToken ct = default)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $@"
                SELECT TOP {pageSize}
                       o.[Order ID]   AS OrderID,
                       o.[Order Date] AS OrderDate,
                       SUM(od.[Unit Price]*od.[Quantity]*(1-od.[Discount])) AS OrderTotal
                FROM Orders AS o
                INNER JOIN [Order Details] AS od ON od.[Order ID]=o.[Order ID]
                WHERE o.[Customer ID]=?
                GROUP BY o.[Order ID], o.[Order Date]
                ORDER BY o.[Order Date] DESC";
            cmd.Parameters.Add("?", OdbcType.Int).Value = customerId;

            var list = new List<OrderHistoryRow>(pageSize);
            using var rd = await cmd.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
            {
                list.Add(new OrderHistoryRow(
                    GetVal<int>(rd, "OrderID"),
                    GetVal<DateTime>(rd, "OrderDate"),
                    GetVal<double>(rd, "OrderTotal")));
            }
            return list;
        }

        public async Task<List<ProductSearchRow>> SearchProductsAsync(
            string? search, int pageSize, CancellationToken ct = default)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $@"
                SELECT TOP {pageSize} p.[ID], p.[Product Name], p.[List Price], p.[Minimum Reorder Quantity]
                FROM Products AS p
                WHERE  p.[Discontinued]=0
                  AND ( ? IS NULL OR p.[Product Name] LIKE '%' & ? & '%' )
                ORDER BY p.[List Price] ASC, p.[ID]";

            cmd.Parameters.Add("?", OdbcType.VarChar).Value = (object?)search ?? DBNull.Value;
            cmd.Parameters.Add("?", OdbcType.VarChar).Value = (object?)search ?? DBNull.Value;

            var list = new List<ProductSearchRow>(pageSize);
            using var rd = await cmd.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
            {
                list.Add(new ProductSearchRow(
                    GetVal<int>(rd, "ID"),
                    GetVal<string>(rd, "Product Name"),
                    GetVal<decimal>(rd, "List Price"),
                    GetValOrNull<short>(rd, "Minimum Reorder Quantity")));
            }
            return list;
        }

        public async Task<int> CreateOrderWithAllocAsync(
    int customerId, int employeeId, int shipperId, decimal shippingFee,
    IEnumerable<NewOrderItem> items, byte allocTypeId = 2, CancellationToken ct = default)
        {
            using var conn = Open();
            using var tx = conn.BeginTransaction();

            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = tx;
                    cmd.CommandText = @"
                        INSERT INTO Orders ([Employee ID],[Customer ID],[Order Date],[Shipper ID],[Shipping Fee])
                        VALUES (?,?,?,?,?)";

                    cmd.Parameters.Add("?", OdbcType.Int).Value = employeeId;
                    cmd.Parameters.Add("?", OdbcType.Int).Value = customerId;
                    cmd.Parameters.Add("?", OdbcType.DateTime).Value = DateTime.UtcNow;
                    cmd.Parameters.Add("?", OdbcType.Int).Value = shipperId;
                    cmd.Parameters.Add("?", OdbcType.NVarChar).Value = shippingFee.ToString("0.0000");

                    await cmd.ExecuteNonQueryAsync(ct);

                    cmd.Parameters.Clear();
                    cmd.CommandText = "SELECT @@IDENTITY";
                    var orderId = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));

                    foreach (var it in items)
                    {
                        float discount = (it.Discount > 1) ? (float)(it.Discount / 100.0) : (float)it.Discount;
                        using var line = conn.CreateCommand();
                        line.Transaction = tx;
                        line.CommandText = @"
                            INSERT INTO [Order Details] ([Order ID],[Product ID],[Quantity],[Unit Price],[Discount])
                            VALUES (?,?,?,?,?)";

                        line.Parameters.Add("?", OdbcType.Int).Value = orderId;
                        line.Parameters.Add("?", OdbcType.Int).Value = it.ProductID;
                        line.Parameters.Add("?", OdbcType.Double).Value = it.Quantity;
                        line.Parameters.Add("?", OdbcType.NVarChar).Value = it.UnitPrice.ToString("0.0000");
                        line.Parameters.Add("?", OdbcType.Real).Value = discount;
                        await line.ExecuteNonQueryAsync(ct);

                        using var inv = conn.CreateCommand();
                        inv.Transaction = tx;
                        inv.CommandText = @"
                            INSERT INTO [Inventory Transactions]
                                ([Transaction Type], [Transaction Created Date], [Product ID], [Quantity], [Customer Order ID], [Comments])
                            VALUES (?,?,?,?,?,?)";

                        inv.Parameters.Add("?", OdbcType.TinyInt).Value = allocTypeId;
                        inv.Parameters.Add("?", OdbcType.DateTime).Value = DateTime.UtcNow;
                        inv.Parameters.Add("?", OdbcType.Int).Value = it.ProductID;
                        inv.Parameters.Add("?", OdbcType.Double).Value = -Math.Abs(it.Quantity);
                        inv.Parameters.Add("?", OdbcType.Int).Value = orderId;
                        inv.Parameters.Add("?", OdbcType.VarChar, 255).Value = "Allocation on order";
                        await inv.ExecuteNonQueryAsync(ct);
                    }

                    tx.Commit();
                    return orderId;
                }
            }
            catch
            {
                try { tx.Rollback(); } catch {  }
                throw;
            }
        }

        public async Task<bool> DeleteOrderAsync(int orderId, CancellationToken ct = default)
        {
            using var conn = Open();
            using var tx = conn.BeginTransaction();

            try
            {
                using (var inv = conn.CreateCommand())
                {
                    inv.Transaction = tx;
                    inv.CommandText = @"
                        DELETE FROM [Inventory Transactions]
                        WHERE [Customer Order ID] = ? AND [Comments] = ?";
                    inv.Parameters.Add("?", OdbcType.Int).Value = orderId;
                    inv.Parameters.Add("?", OdbcType.VarChar, 255).Value = "Allocation on order";
                    await inv.ExecuteNonQueryAsync(ct);
                }

                using (var line = conn.CreateCommand())
                {
                    line.Transaction = tx;
                    line.CommandText = @"DELETE FROM [Order Details] WHERE [Order ID] = ?";
                    line.Parameters.Add("?", OdbcType.Int).Value = orderId;
                    await line.ExecuteNonQueryAsync(ct);
                }

                int affected;
                using (var hdr = conn.CreateCommand())
                {
                    hdr.Transaction = tx;
                    hdr.CommandText = @"DELETE FROM Orders WHERE [Order ID] = ?";
                    hdr.Parameters.Add("?", OdbcType.Int).Value = orderId;
                    affected = await hdr.ExecuteNonQueryAsync(ct);
                }

                tx.Commit();
                return affected > 0;
            }
            catch
            {
                try { tx.Rollback(); } catch {}
                throw;
            }
        }

        public async Task<List<SalesByDay>> GetDailySalesAsync(DateTime fromUtc, DateTime toUtc, CancellationToken ct = default)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                SELECT
                    DateValue([Orders].[Order Date]) AS OrderDay,
                    SUM([Order Details].[Unit Price] * [Order Details].[Quantity] *
                        (1 - IIf([Order Details].[Discount] Is Null, 0, [Order Details].[Discount]))) AS Sales
                FROM ([Orders]
                      INNER JOIN [Order Details] ON [Orders].[Order ID] = [Order Details].[Order ID])
                WHERE [Orders].[Order Date] >= ? AND [Orders].[Order Date] < ?
                GROUP BY DateValue([Orders].[Order Date])
                ORDER BY 1";
            cmd.Parameters.Add("?", OdbcType.DateTime).Value = fromUtc;
            cmd.Parameters.Add("?", OdbcType.DateTime).Value = toUtc;

            var list = new List<SalesByDay>(128);
            using var rd = await cmd.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
            {
                list.Add(new SalesByDay(
                    (DateTime)rd["OrderDay"],
                    Convert.ToDouble(rd["Sales"])
                ));
            }
            return list;
        }

        public async Task<List<TopProductRow>> GetTopProductsAsync(DateTime fromUtc, DateTime toUtc, int topN, CancellationToken ct = default)
        {
            if (topN <= 0) topN = 10;
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $@"
                SELECT TOP {topN}
                    [Products].[ID]            AS [Product ID],
                    [Products].[Product Name]  AS [Product Name],
                    [Products].[Category]      AS [Category],
                    SUM(
                        [Order Details].[Unit Price] *
                        [Order Details].[Quantity] *
                        (1 - IIf([Order Details].[Discount] Is Null, 0, [Order Details].[Discount]))
                    ) AS [Revenue]
                FROM (([Orders]
                        INNER JOIN [Order Details]
                            ON [Orders].[Order ID] = [Order Details].[Order ID])
                        INNER JOIN [Products]
                            ON [Products].[ID] = [Order Details].[Product ID])
                WHERE [Orders].[Order Date] >= ? AND [Orders].[Order Date] < ?
                GROUP BY
                    [Products].[ID],
                    [Products].[Product Name],
                    [Products].[Category]
                ORDER BY 4 DESC";
            cmd.Parameters.Add("?", OdbcType.DateTime).Value = fromUtc;
            cmd.Parameters.Add("?", OdbcType.DateTime).Value = toUtc;

            var list = new List<TopProductRow>(topN);
            using var rd = await cmd.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
            {
                list.Add(new TopProductRow(
                    GetVal<int>(rd, "Product ID"),
                    GetVal<string>(rd, "Product Name"),
                    GetVal<string>(rd, "Category"),
                    GetVal<double>(rd, "Revenue")));
            }
            return list;
        }

        public async Task<List<ReorderSuggestionRow>> GetReorderSuggestionsAsync(CancellationToken ct = default)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            var from30 = DateTime.UtcNow.AddDays(-30);

            cmd.CommandText = @"
                SELECT
                    p.[ID] AS [Product ID],
                    p.[Product Name],
                    s.OnHand,
                    p.[Target Level] AS [TargetLevel],
                    p.[Minimum Reorder Quantity] AS [MinimumReorder],
                    IIf(l.QtyOut30d Is Null, 0, l.QtyOut30d) AS [Last30dOut]
                FROM (
                      (
                        Products AS p
                        LEFT JOIN
                        (
                            SELECT it.[Product ID] AS ProductID,
                                   SUM(it.[Quantity]) AS OnHand
                            FROM [Inventory Transactions] AS it
                            GROUP BY it.[Product ID]
                        ) AS s
                        ON p.[ID] = s.ProductID
                      )
                      LEFT JOIN
                      (
                          SELECT it2.[Product ID] AS ProductID,
                                 SUM(IIf(it2.[Quantity] < 0, -it2.[Quantity], 0)) AS QtyOut30d
                          FROM [Inventory Transactions] AS it2
                          WHERE it2.[Transaction Created Date] >= ?
                          GROUP BY it2.[Product ID]
                      ) AS l
                      ON p.[ID] = l.ProductID
                )
                WHERE s.OnHand Is Null OR s.OnHand < p.[Target Level]
                ORDER BY IIf(l.QtyOut30d Is Null, 0, l.QtyOut30d) DESC, p.[ID]";

            cmd.Parameters.Add("?", OdbcType.DateTime).Value = from30;

            var list = new List<ReorderSuggestionRow>(100);
            using var rd = await cmd.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
            {
                var onHand = GetValOrNull<double>(rd, "OnHand");
                list.Add(new ReorderSuggestionRow(
                    GetVal<int>(rd, "Product ID"),
                    GetVal<string>(rd, "Product Name"),
                    onHand.HasValue ? (int?)Convert.ToInt32(onHand.Value) : null,
                    GetValOrNull<int>(rd, "TargetLevel"),
                    GetValOrNull<short>(rd, "MinimumReorder"),
                    Convert.ToInt32(GetVal<double>(rd, "Last30dOut"))));
            }
            return list;
        }

        public async Task<int> BumpOrderLineQuantityAsync(int orderDetailId, double delta, CancellationToken ct = default)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                UPDATE [Order Details]
                SET [Quantity] = [Quantity] + ?
                WHERE [ID] = ?";
            cmd.Parameters.Add("?", OdbcType.Double).Value = delta;
            cmd.Parameters.Add("?", OdbcType.Int).Value = orderDetailId;
            return await cmd.ExecuteNonQueryAsync(ct);
        }

        public string GetProvider() => "Access";
    }
}
