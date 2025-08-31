using WebApplication11.Interfaces;
using Microsoft.Data.SqlClient;
using static WebApplication11.Models.Models;
using System.Data;

namespace WebApplication11.Providers
{
    public class SqlServerProvider : IDataProvider
    {
        private async static Task<SqlConnection> Open()
        {
            var c = new SqlConnection("Server=(localdb)\\MSSQLLocalDB;Database=AccessTestNew;Trusted_Connection=True;");
            await c.OpenAsync();
            return c;
        }

        private static T GetVal<T>(SqlDataReader r, string name)
            => r.GetFieldValue<T>(r.GetOrdinal(name));
        private static T? GetValOrNull<T>(SqlDataReader r, string name) where T : struct
        {
            int i = r.GetOrdinal(name);
            return r.IsDBNull(i) ? null : r.GetFieldValue<T>(i);
        }
        private static string? GetStrOrNull(SqlDataReader r, string name)
        {
            int i = r.GetOrdinal(name);
            return r.IsDBNull(i) ? null : r.GetString(i);
        }

        public async Task<OrderDto?> GetOrderWithLinesAsync(int orderId, CancellationToken ct = default)
        {
            using var conn = await Open();
            using var cmd = new SqlCommand(@"
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
                FROM [Orders] o
                JOIN [Order Details] od ON od.[Order ID] = o.[Order ID]
                JOIN [Products] p       ON p.[ID]       = od.[Product ID]
                WHERE o.[Order ID] = @OrderID
                ORDER BY od.[ID];", conn);
            cmd.Parameters.Add("@OrderID", SqlDbType.Int).Value = orderId;

            using var rd = await cmd.ExecuteReaderAsync(ct);
            if (!rd.HasRows) return null;

            await rd.ReadAsync(ct);

            var header = new
            {
                OrderID = GetVal<int>(rd, "OrderID"),
                CustomerID = GetVal<int>(rd, "CustomerID"),
                EmployeeID = GetVal<int>(rd, "EmployeeID"),
                OrderDate = GetVal<DateTime>(rd, "OrderDate"),
                ShipperID = GetVal<int>(rd, "ShipperID"),
                ShippingFee = GetVal<decimal>(rd, "ShippingFee")
            };

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

            return new OrderDto(header.OrderID, header.CustomerID, header.EmployeeID, header.OrderDate,
                                header.ShipperID, header.ShippingFee, lines);
        }

        public async Task<List<OrderHistoryRow>> GetCustomerOrderHistoryAsync(
            int customerId, int pageSize, CancellationToken ct = default)
        {
            using var conn = await Open();
            using var cmd = new SqlCommand(@"
                SELECT o.[Order ID] AS OrderID, o.[Order Date] AS OrderDate,
                       SUM(od.[Unit Price]*od.[Quantity]*(1-od.[Discount])) AS OrderTotal
                FROM [Orders] o
                JOIN [Order Details] od ON od.[Order ID] = o.[Order ID]
                WHERE o.[Customer ID] = @CustomerID
                GROUP BY o.[Order ID], o.[Order Date]
                ORDER BY o.[Order Date] DESC
                OFFSET 0 ROWS FETCH NEXT @PageSize ROWS ONLY;", conn);
            cmd.Parameters.Add("@CustomerID", SqlDbType.Int).Value = customerId;
            cmd.Parameters.Add("@PageSize", SqlDbType.Int).Value = pageSize;

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
            using var conn = await Open();
            using var cmd = new SqlCommand(@"
                SELECT p.[ID], p.[Product Name], p.[List Price], p.[Minimum Reorder Quantity]
                FROM [Products] p
                WHERE p.[Discontinued] = 0
                  AND (@Search IS NULL OR p.[Product Name] LIKE '%' + @Search + '%')
                ORDER BY p.[List Price] ASC, p.[ID]
                OFFSET 0 ROWS FETCH NEXT @PageSize ROWS ONLY;", conn);
            cmd.Parameters.Add("@Search", SqlDbType.NVarChar, 255).Value = (object?)search ?? DBNull.Value;
            cmd.Parameters.Add("@PageSize", SqlDbType.Int).Value = pageSize;

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
            using var conn = await Open();
            using var tx = (SqlTransaction)await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);

            try
            {
                using var insertOrder = new SqlCommand(@"
                    INSERT INTO [Orders] ([Employee ID], [Customer ID], [Order Date], [Shipper ID], [Shipping Fee])
                    VALUES (@Emp, @Cust, SYSUTCDATETIME(), @ShipperID, @ShippingFee);
                    SELECT CAST(SCOPE_IDENTITY() AS int);", conn, tx);
                insertOrder.Parameters.Add("@Emp", SqlDbType.Int).Value = employeeId;
                insertOrder.Parameters.Add("@Cust", SqlDbType.Int).Value = customerId;
                insertOrder.Parameters.Add("@ShipperID", SqlDbType.Int).Value = shipperId;
                insertOrder.Parameters.Add("@ShippingFee", SqlDbType.Money).Value = shippingFee;

                var orderId = (int)(await insertOrder.ExecuteScalarAsync(ct))!;

                const string insertLineSql = @"
                    INSERT INTO [Order Details] ([Order ID], [Product ID], [Quantity], [Unit Price], [Discount])
                    VALUES (@OrderID, @ProdID, @Qty, @UnitPrice, @Discount);";

                foreach (var it in items)
                {
                    float discount = (it.Discount > 1) ? (float)(it.Discount / 100.0) : (float)it.Discount;
                    using var lineCmd = new SqlCommand(insertLineSql, conn, tx);
                    lineCmd.Parameters.Add("@OrderID", SqlDbType.Int).Value = orderId;
                    lineCmd.Parameters.Add("@ProdID", SqlDbType.Int).Value = it.ProductID;
                    lineCmd.Parameters.Add("@Qty", SqlDbType.Float).Value = it.Quantity;
                    lineCmd.Parameters.Add("@UnitPrice", SqlDbType.Money).Value = it.UnitPrice;
                    lineCmd.Parameters.Add("@Discount", SqlDbType.Real).Value = discount;
                    await lineCmd.ExecuteNonQueryAsync(ct);
                }

                const string invSql = @"
                    INSERT INTO [Inventory Transactions]
                        ([Transaction Type], [Transaction Created Date], [Product ID], [Quantity], [Customer Order ID], [Comments])
                    VALUES (@Type, SYSUTCDATETIME(), @ProdID, @NegQty, @OrderID, @Cmt);";

                foreach (var it in items)
                {
                    using var invCmd = new SqlCommand(invSql, conn, tx);
                    invCmd.Parameters.Add("@Type", SqlDbType.TinyInt).Value = allocTypeId;
                    invCmd.Parameters.Add("@ProdID", SqlDbType.Int).Value = it.ProductID;
                    invCmd.Parameters.Add("@NegQty", SqlDbType.Float).Value = -Math.Abs(it.Quantity);
                    invCmd.Parameters.Add("@OrderID", SqlDbType.Int).Value = orderId;
                    invCmd.Parameters.Add("@Cmt", SqlDbType.NVarChar, 255).Value = "Allocation on order";
                    await invCmd.ExecuteNonQueryAsync(ct);
                }

                await tx.CommitAsync(ct);
                return orderId;
            }
            catch
            {
                try { await tx.RollbackAsync(ct); } catch { }
                throw;
            }
        }

        public async Task<List<SalesByDay>> GetDailySalesAsync(DateTime fromUtc, DateTime toUtc, CancellationToken ct = default)
        {
            using var conn = await Open();
            using var cmd = new SqlCommand(@"
                SELECT CAST(o.[Order Date] AS date) AS [Day],
                       SUM(od.[Unit Price]*od.[Quantity]*(1-od.[Discount])) AS [Sales]
                FROM [Orders] o
                JOIN [Order Details] od ON od.[Order ID] = o.[Order ID]
                WHERE o.[Order Date] >= @From AND o.[Order Date] < @To
                GROUP BY CAST(o.[Order Date] AS date)
                ORDER BY [Day];", conn);
            cmd.Parameters.Add("@From", SqlDbType.DateTime2).Value = fromUtc;
            cmd.Parameters.Add("@To", SqlDbType.DateTime2).Value = toUtc;

            var list = new List<SalesByDay>(256);
            using var rd = await cmd.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
                list.Add(new SalesByDay(GetVal<DateTime>(rd, "Day"), GetVal<double>(rd, "Sales")));
            return list;
        }

        public async Task<List<TopProductRow>> GetTopProductsAsync(DateTime fromUtc, DateTime toUtc, int topN, CancellationToken ct = default)
        {
            using var conn = await Open();
            using var cmd = new SqlCommand(@"
                SELECT TOP (@TopN)
                    p.[ID] AS [Product ID], p.[Product Name], p.[Category],
                    SUM(od.[Unit Price]*od.[Quantity]*(1-od.[Discount])) AS [Revenue]
                FROM [Order Details] od
                JOIN [Orders]  o ON o.[Order ID] = od.[Order ID]
                JOIN [Products] p ON p.[ID] = od.[Product ID]
                WHERE o.[Order Date] >= @From AND o.[Order Date] < @To
                GROUP BY p.[ID], p.[Product Name], p.[Category]
                ORDER BY [Revenue] DESC;", conn);
            cmd.Parameters.Add("@From", SqlDbType.DateTime2).Value = fromUtc;
            cmd.Parameters.Add("@To", SqlDbType.DateTime2).Value = toUtc;
            cmd.Parameters.Add("@TopN", SqlDbType.Int).Value = topN;

            var list = new List<TopProductRow>(topN);
            using var rd = await cmd.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
                list.Add(new TopProductRow(
                    GetVal<int>(rd, "Product ID"),
                    GetVal<string>(rd, "Product Name"),
                    GetVal<string>(rd, "Category"),
                    GetVal<double>(rd, "Revenue")));
            return list;
        }

        public async Task<List<ReorderSuggestionRow>> GetReorderSuggestionsAsync(CancellationToken ct = default)
        {
            using var conn = await Open();
            using var cmd = new SqlCommand(@"
                WITH last30 AS (
                    SELECT it.[Product ID] AS ProductID,
                           SUM(CASE WHEN it.[Quantity] < 0 THEN -it.[Quantity] ELSE 0 END) AS QtyOut30d
                    FROM [Inventory Transactions] it
                    WHERE it.[Transaction Created Date] >= DATEADD(day, -30, SYSUTCDATETIME())
                    GROUP BY it.[Product ID]
                ),
                stock AS (
                    SELECT it.[Product ID] AS ProductID,
                           SUM(it.[Quantity]) AS OnHand
                    FROM [Inventory Transactions] it
                    GROUP BY it.[Product ID]
                )
                SELECT p.[ID] AS ProductID, p.[Product Name] AS ProductName,
                       s.OnHand, p.[Target Level] AS TargetLevel, p.[Minimum Reorder Quantity] AS MinimumReorder,
                       ISNULL(l.QtyOut30d, 0) AS Last30dOut
                FROM [Products] p
                LEFT JOIN stock  s ON s.ProductID = p.[ID]
                LEFT JOIN last30 l ON l.ProductID = p.[ID]
                WHERE s.OnHand IS NULL OR s.OnHand < p.[Target Level]
                ORDER BY Last30dOut DESC, p.[ID]
                OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY;", conn);

            var list = new List<ReorderSuggestionRow>(100);
            using var rd = await cmd.ExecuteReaderAsync(ct);
            while (await rd.ReadAsync(ct))
                list.Add(new ReorderSuggestionRow(
                    GetVal<int>(rd, "ProductID"),
                    GetVal<string>(rd, "ProductName"),
                    GetValOrNull<int>(rd, "OnHand"),
                    GetValOrNull<int>(rd, "TargetLevel"),
                    GetValOrNull<short>(rd, "MinimumReorder"),
                    GetVal<int>(rd, "Last30dOut")));
            return list;
        }

        public async Task<int> BumpOrderLineQuantityAsync(int orderDetailId, double delta, CancellationToken ct = default)
        {
            using var conn = await Open();
            using var cmd = new SqlCommand(@"
                UPDATE [Order Details]
                SET [Quantity] = [Quantity] + @Delta
                WHERE [ID] = @OrderDetailID;", conn);
            cmd.Parameters.Add("@Delta", SqlDbType.Float).Value = delta;
            cmd.Parameters.Add("@OrderDetailID", SqlDbType.Int).Value = orderDetailId;
            return await cmd.ExecuteNonQueryAsync(ct);
        }

    public string GetProvider()
        {
            return "SqlServer";
        }

        public async Task<bool> DeleteOrderAsync(int orderId, CancellationToken ct = default)
        {

            using var conn = await Open();
            await using var tx = await conn.BeginTransactionAsync(ct);

            try
            {
                await using (var inv = conn.CreateCommand())
                {
                    inv.Transaction = (SqlTransaction)tx;
                    inv.CommandText = @"
                        DELETE FROM [dbo].[Inventory Transactions]
                        WHERE [Customer Order ID] = @OrderId AND [Comments] = @Comments";
                    inv.Parameters.Add(new SqlParameter("@OrderId", SqlDbType.Int) { Value = orderId });
                    inv.Parameters.Add(new SqlParameter("@Comments", SqlDbType.VarChar, 255) { Value = "Allocation on order" });
                    await inv.ExecuteNonQueryAsync(ct);
                }

                await using (var line = conn.CreateCommand())
                {
                    line.Transaction = (SqlTransaction)tx;
                    line.CommandText = @"DELETE FROM [dbo].[Order Details] WHERE [Order ID] = @OrderId";
                    line.Parameters.Add(new SqlParameter("@OrderId", SqlDbType.Int) { Value = orderId });
                    await line.ExecuteNonQueryAsync(ct);
                }

                int affected;
                await using (var hdr = conn.CreateCommand())
                {
                    hdr.Transaction = (SqlTransaction)tx;
                    hdr.CommandText = @"DELETE FROM [dbo].[Orders] WHERE [Order ID] = @OrderId";
                    hdr.Parameters.Add(new SqlParameter("@OrderId", SqlDbType.Int) { Value = orderId });
                    affected = await hdr.ExecuteNonQueryAsync(ct);
                }

                await tx.CommitAsync(ct);
                return affected > 0;
            }
            catch
            {
                try { await tx.RollbackAsync(ct); } catch { }
                throw;
            }
        }
    }
}