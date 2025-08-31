using MongoDB.Bson;
using MongoDB.Driver;
using System.Globalization;
using WebApplication11.Interfaces;
using static WebApplication11.Models.Models;

namespace WebApplication11.Providers
{
    public class MongoDbProvider : IDataProvider
    {
        private readonly IMongoDatabase _db;
        private readonly IMongoCollection<BsonDocument> _orders;
        private readonly IMongoCollection<BsonDocument> _products;

        public MongoDbProvider()
        {
            var client = new MongoClient("mongodb://localhost:27017");
            _db = client.GetDatabase("Northwind");

            _orders = _db.GetCollection<BsonDocument>("orders");
            _products = _db.GetCollection<BsonDocument>("products");
        }

        private static int BInt(BsonValue v, int def = 0) => v?.IsBsonNull == false ? v.ToInt32() : def;
        private static double BDbl(BsonValue v, double def = 0.0) => v?.IsBsonNull == false ? v.ToDouble() : def;
        private static decimal BDec(BsonValue v, decimal def = 0m) => v?.IsBsonNull == false ? (v.IsDecimal128 ? Decimal128.ToDecimal(v.AsDecimal128) : Convert.ToDecimal(BDbl(v))) : def;
        private static string? BStr(BsonValue v) => v?.IsBsonNull == false ? v.ToString() : null;
        private static DateTime BDt(BsonValue v)
        {
            if (v == null || v.IsBsonNull) return DateTime.MinValue;

            switch (v.BsonType)
            {
                case BsonType.DateTime:
                    return v.ToUniversalTime();

                case BsonType.String:
                    {
                        var s = v.AsString;
                        if (DateTimeOffset.TryParse(
                                s, CultureInfo.InvariantCulture,
                                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                                out var dto))
                            return dto.UtcDateTime;

                        if (long.TryParse(s, out var n))
                        {
                            if (n > 1_000_000_000_000) return DateTimeOffset.FromUnixTimeMilliseconds(n).UtcDateTime;
                            if (n > 10_000_000) return DateTimeOffset.FromUnixTimeSeconds(n).UtcDateTime;
                        }
                        return DateTime.SpecifyKind(DateTime.Parse(s, CultureInfo.InvariantCulture), DateTimeKind.Utc);
                    }

                case BsonType.Int64:
                    {
                        var n = v.AsInt64;
                        return (n > 1_000_000_000_000)
                            ? DateTimeOffset.FromUnixTimeMilliseconds(n).UtcDateTime
                            : DateTimeOffset.FromUnixTimeSeconds(n).UtcDateTime;
                    }

                case BsonType.Int32:
                    return DateTimeOffset.FromUnixTimeSeconds(v.AsInt32).UtcDateTime;

                case BsonType.Timestamp:
                    var ts = v.AsBsonTimestamp;
                    return DateTimeOffset.FromUnixTimeSeconds(ts.Timestamp).UtcDateTime;

                default:
                    return DateTime.MinValue;
            }
        }

        public string GetProvider() => "MongoDB";

        public async Task<OrderDto?> GetOrderWithLinesAsync(int orderId, CancellationToken ct = default)
        {
            var pipeline = new[]
            {
                new BsonDocument("$match", new BsonDocument("orderId", orderId)),
                new BsonDocument("$unwind", "$orderDetails"),
                new BsonDocument("$lookup", new BsonDocument {
                    { "from", "products" }, { "localField", "orderDetails.productId" }, { "foreignField", "id" }, { "as", "p" }
                }),
                new BsonDocument("$unwind", "$p"),
                new BsonDocument("$group", new BsonDocument {
                    { "_id", "$orderId" },
                    { "order", new BsonDocument("$first", "$$ROOT") },
                    { "lines", new BsonDocument("$push", new BsonDocument {
                        { "productId", "$orderDetails.productId" },
                        { "unitPrice", "$orderDetails.unitPrice" },
                        { "quantity",  "$orderDetails.quantity" },
                        { "discount",  "$orderDetails.discount" },
                        { "productName", "$p.productName" }
                    })}
                }),
                new BsonDocument("$project", new BsonDocument {
                    { "_id", 0 },
                    { "orderId", "$_id" },
                    { "customerId", "$order.customerId" },
                    { "employeeId", "$order.employeeId" },
                    { "orderDate",  "$order.orderDate" },
                    { "shipperId",  "$order.shipperId" },
                    { "shippingFee","$order.shippingFee" },
                    { "lines", "$lines" }
                })
            };

            var d = await _orders.Aggregate<BsonDocument>(pipeline, cancellationToken: ct).FirstOrDefaultAsync(ct);
            if (d is null) return null;

            var lines = d["lines"].AsBsonArray.Select(l => new OrderLineDto(
                BInt(l["productId"]),
                BDec(l["unitPrice"]),
                BDbl(l["quantity"]),
                BDbl(l["discount"]),
                l["productName"].AsString
            )).ToList();

            return new OrderDto(
                BInt(d["orderId"]),
                BInt(d["customerId"]),
                BInt(d["employeeId"]),
                BDt(d["orderDate"]),
                BInt(d["shipperId"]),
                BDec(d["shippingFee"]),
                lines
            );
        }

        public async Task<List<OrderHistoryRow>> GetCustomerOrderHistoryAsync(int customerId, int pageSize, CancellationToken ct = default)
        {
            var pipeline = new List<BsonDocument>
            {
                new("$match", new BsonDocument("customerId", customerId)),
                new("$project", new BsonDocument {
                    { "orderId", 1 }, { "orderDate", 1 },
                    { "total", new BsonDocument("$sum", new BsonDocument("$map", new BsonDocument {
                        { "input", "$orderDetails" }, { "as", "i" },
                        { "in", new BsonDocument("$multiply", new BsonArray {
                            "$$i.unitPrice", "$$i.quantity",
                            new BsonDocument("$subtract", new BsonArray{ 1, "$$i.discount" })
                        })}
                    }))}
                }),
                new("$sort", new BsonDocument("orderDate", -1)),
                new("$limit", pageSize)
            };

            var list = new List<OrderHistoryRow>(pageSize);
            using var cur = await _orders.AggregateAsync<BsonDocument>(pipeline, cancellationToken: ct);
            await cur.ForEachAsync(d => list.Add(new OrderHistoryRow(
                BInt(d["orderId"]),
                BDt(d["orderDate"]),
                BDbl(d["total"])
            )), ct);
            return list;
        }

        public async Task<List<ProductSearchRow>> SearchProductsAsync(string? search, int pageSize, CancellationToken ct = default)
        {
            var f = Builders<BsonDocument>.Filter.Eq("discontinued", false);
            if (!string.IsNullOrWhiteSpace(search))
                f &= Builders<BsonDocument>.Filter.Regex("productName", new BsonRegularExpression(search, "i"));

            var docs = await _products.Find(f)
                                      .Sort(Builders<BsonDocument>.Sort.Ascending("listPrice").Ascending("id"))
                                      .Limit(pageSize)
                                      .ToListAsync(ct);

            return docs.Select(p => new ProductSearchRow(
                BInt(p.GetValue("id", 0)),
                BStr(p.GetValue("productName", "")) ?? "",
                BDec(p.GetValue("listPrice", 0)),
                p.TryGetValue("minimumReorderQuantity", out var mrq) && !mrq.IsBsonNull
                    ? (short?)Convert.ToInt16(mrq.ToInt32())
                    : null
            )).ToList();
        }

        public async Task<int> CreateOrderWithAllocAsync(
    int customerId, int employeeId, int shipperId, decimal shippingFee,
    IEnumerable<NewOrderItem> items, byte allocTypeId = 2, CancellationToken ct = default)
        {
            var counters = _db.GetCollection<BsonDocument>("counters");

            var maxDoc = await _orders.Aggregate()
                .Group(new BsonDocument { { "_id", BsonNull.Value }, { "max", new BsonDocument("$max", "$orderId") } })
                .FirstOrDefaultAsync(ct);
            var maxOrderId = maxDoc?.GetValue("max", 0).ToInt64() ?? 0L;

            await counters.UpdateOneAsync(
                Builders<BsonDocument>.Filter.Eq("_id", "orderId"),
                Builders<BsonDocument>.Update.Max("seq", maxOrderId),
                new UpdateOptions { IsUpsert = true },
                ct
            );

            var next = await counters.FindOneAndUpdateAsync(
                Builders<BsonDocument>.Filter.Eq("_id", "orderId"),
                Builders<BsonDocument>.Update.Inc("seq", 1),
                new FindOneAndUpdateOptions<BsonDocument>
                {
                    IsUpsert = true,
                    ReturnDocument = ReturnDocument.After
                },
                ct
            );

            int newOrderId = checked((int)next["seq"].ToInt64());

            var order = new BsonDocument
            {
                { "orderId", newOrderId },
                { "customerId", customerId },
                { "employeeId", employeeId },
                { "orderDate", DateTime.UtcNow },
                { "shipperId", shipperId },
                { "shippingFee", shippingFee },
                { "orderDetails", new BsonArray(items.Select(i => new BsonDocument {
                    { "orderId",  newOrderId },
                    { "productId", i.ProductID },
                    { "unitPrice", i.UnitPrice },
                    { "quantity",  i.Quantity },
                    { "discount",  i.Discount },
                    { "statusId",  0 }
                })) }
            };

            await _orders.InsertOneAsync(order, cancellationToken: ct);

            var ops = new List<WriteModel<BsonDocument>>();
            foreach (var it in items)
            {
                var filter = Builders<BsonDocument>.Filter.Eq("id", it.ProductID);
                var txDoc = new BsonDocument
                {
                    { "transactionType", allocTypeId },
                    { "transactionCreatedDate", DateTime.UtcNow },
                    { "productId", it.ProductID },
                    { "quantity", -Math.Abs(it.Quantity) },
                    { "orderId", newOrderId },
                    { "comments", "Allocation on order" }
                };
                var update = Builders<BsonDocument>.Update.Push("inventoryTransactions", txDoc);
                ops.Add(new UpdateOneModel<BsonDocument>(filter, update));
            }

            if (ops.Count > 0)
            {
                try
                {
                    await _products.BulkWriteAsync(ops, cancellationToken: ct);
                }
                catch
                {
                    await _orders.DeleteOneAsync(
                        Builders<BsonDocument>.Filter.Eq("orderId", newOrderId), ct);
                    throw;
                }
            }

            return newOrderId;
        }

        public async Task<List<SalesByDay>> GetDailySalesAsync(
    DateTime fromUtc, DateTime toUtc, CancellationToken ct = default)
        {
            var pipeline = new[]
            {
                new BsonDocument("$addFields",
                    new BsonDocument("orderDateDT",
                        new BsonDocument("$toDate", "$orderDate"))),

                new BsonDocument("$match",
                    new BsonDocument("orderDateDT",
                        new BsonDocument { { "$gte", fromUtc }, { "$lt", toUtc } })),

                new BsonDocument("$unwind", "$orderDetails"),

                new BsonDocument("$group", new BsonDocument
                {
                    { "_id", new BsonDocument("$dateTrunc",
                        new BsonDocument { { "date", "$orderDateDT" }, { "unit", "day" } }) },
                    { "sales", new BsonDocument("$sum",
                        new BsonDocument("$multiply", new BsonArray {
                            "$orderDetails.unitPrice",
                            "$orderDetails.quantity",
                            new BsonDocument("$subtract", new BsonArray { 1, "$orderDetails.discount" })
                        }))
                    }
                }),

                new BsonDocument("$sort", new BsonDocument("_id", 1)),
                new BsonDocument("$project",
                    new BsonDocument { { "day", "$_id" }, { "sales", 1 }, { "_id", 0 } })
            };

            var list = new List<SalesByDay>(128);
            using var cur = await _orders.AggregateAsync<BsonDocument>(pipeline, cancellationToken: ct);
            await cur.ForEachAsync(d => list.Add(new SalesByDay(
                BDt(d["day"]), BDbl(d["sales"]))), ct);

            return list;
        }

        public async Task<List<TopProductRow>> GetTopProductsAsync(DateTime fromUtc, DateTime toUtc, int topN, CancellationToken ct = default)
        {
            if (topN <= 0) topN = 10;

            var pipeline = new[]
            {
                new BsonDocument("$addFields", new BsonDocument("orderDateDT",
                    new BsonDocument("$toDate", "$orderDate"))),

                new BsonDocument("$match", new BsonDocument("orderDateDT",
                    new BsonDocument { { "$gte", fromUtc }, { "$lt", toUtc } })),

                new BsonDocument("$unwind", "$orderDetails"),

                new BsonDocument("$group", new BsonDocument
                {
                    { "_id", "$orderDetails.productId" },
                    { "revenue", new BsonDocument("$sum",
                        new BsonDocument("$multiply", new BsonArray {
                            "$orderDetails.unitPrice",
                            "$orderDetails.quantity",
                            new BsonDocument("$subtract", new BsonArray { 1, "$orderDetails.discount" })
                        }))
                    }
                }),

                new BsonDocument("$sort", new BsonDocument("revenue", -1)),
                new BsonDocument("$limit", topN),

                new BsonDocument("$lookup", new BsonDocument
                {
                    { "from", "products" },
                    { "localField", "_id" },
                    { "foreignField", "id" },
                    { "as", "p" }
                }),
                new BsonDocument("$unwind", "$p"),

                new BsonDocument("$project", new BsonDocument
                {
                    { "_id", 0 },
                    { "productId", "$_id" },
                    { "productName", "$p.productName" },
                    { "category", "$p.category" },
                    { "revenue", "$revenue" }
                })
            };

            var list = new List<TopProductRow>(topN);
            using var cur = await _orders.AggregateAsync<BsonDocument>(pipeline, cancellationToken: ct);
            await cur.ForEachAsync(d => list.Add(new TopProductRow(
                BInt(d["productId"]),
                d["productName"].AsString,
                d["category"].AsString,
                BDbl(d["revenue"])
            )), ct);
            return list;
        }

        public async Task<List<ReorderSuggestionRow>> GetReorderSuggestionsAsync(CancellationToken ct = default)
        {
            var from30 = DateTime.UtcNow.AddDays(-30);

            var pipeline = new[]
            {
                new BsonDocument("$project", new BsonDocument
                {
                    { "id", 1 },
                    { "productName", "$productName" },
                    { "targetLevel", "$targetLevel" },
                    { "minimumReorder", "$minimumReorderQuantity" },
                    { "tx", new BsonDocument("$ifNull", new BsonArray { "$inventoryTransactions", new BsonArray() }) }
                }),

                new BsonDocument("$addFields", new BsonDocument
                {
                    { "onHand", new BsonDocument("$sum",
                        new BsonDocument("$map", new BsonDocument
                        {
                            { "input", "$tx" }, { "as", "t" },
                            { "in", new BsonDocument("$ifNull", new BsonArray { "$$t.quantity", 0 }) }
                        }))
                    },
                    { "last30dOut", new BsonDocument("$sum",
                        new BsonDocument("$map", new BsonDocument
                        {
                            { "input", new BsonDocument("$filter", new BsonDocument
                                {
                                    { "input", "$tx" }, { "as", "t" },
                                    { "cond", new BsonDocument("$and", new BsonArray
                                        {
                                            new BsonDocument("$lt", new BsonArray { "$$t.quantity", 0 }),
                                            new BsonDocument("$gte", new BsonArray
                                                {
                                                    new BsonDocument("$toDate", "$$t.transactionCreatedDate"),
                                                    from30
                                                })
                                        })
                                    }
                                })
                            },
                            { "as", "t" },
                            { "in", new BsonDocument("$abs", "$$t.quantity") }
                        }))
                    },
                    { "onHand0", new BsonDocument("$ifNull", new BsonArray { "$onHand", 0 }) },
                    { "targetLevel0", new BsonDocument("$ifNull", new BsonArray { "$targetLevel", 0 }) }
                }),

                new BsonDocument("$match", new BsonDocument("$expr",
                new BsonDocument("$lt", new BsonArray { "$onHand0", "$targetLevel0" }))),

                new BsonDocument("$project", new BsonDocument
                {
                    { "_id", 0 },
                    { "productId", "$id" },
                    { "productName", 1 },
                    { "onHand", "$onHand0" },
                    { "targetLevel", "$targetLevel0" },
                    { "minimumReorder", 1 },
                    { "last30dOut", 1 }
                }),
                new BsonDocument("$sort", new BsonDocument { { "last30dOut", -1 }, { "productId", 1 } }),
                new BsonDocument("$limit", 100)
            };

            var list = new List<ReorderSuggestionRow>(100);
            using var cur = await _products.AggregateAsync<BsonDocument>(pipeline, cancellationToken: ct);
            await cur.ForEachAsync(d => list.Add(new ReorderSuggestionRow(
                BInt(d["productId"]),
                d.GetValue("productName", BsonNull.Value).IsBsonNull ? "" : d["productName"].AsString,
                d.TryGetValue("onHand", out var oh) && !oh.IsBsonNull ? (int?)Convert.ToInt32(BDbl(oh)) : null,
                d.TryGetValue("targetLevel", out var tl) && !tl.IsBsonNull ? (int?)tl.ToInt32() : null,
                d.TryGetValue("minimumReorder", out var mr) && !mr.IsBsonNull ? (short?)Convert.ToInt16(BInt(mr)) : null,
                Convert.ToInt32(BDbl(d.GetValue("last30dOut", 0)))
            )), ct);
            return list;
        }

        public async Task<int> BumpOrderLineQuantityAsync(int orderDetailId, double delta, CancellationToken ct = default)
        {
            var filter1 = Builders<BsonDocument>.Filter.Eq("orderDetails.id", orderDetailId);
            var update = Builders<BsonDocument>.Update.Inc("orderDetails.$[i].quantity", delta);
            var opts = new UpdateOptions
            {
                ArrayFilters = new[] { new BsonDocumentArrayFilterDefinition<BsonDocument>(new BsonDocument("i.id", orderDetailId)) }
            };
            var res = await _orders.UpdateOneAsync(filter1, update, opts, ct);
            if (res.ModifiedCount > 0) return (int)res.ModifiedCount;

            var filter2 = Builders<BsonDocument>.Filter.Eq("orderDetails.productId", orderDetailId);
            var opts2 = new UpdateOptions
            {
                ArrayFilters = new[] { new BsonDocumentArrayFilterDefinition<BsonDocument>(new BsonDocument("i.productId", orderDetailId)) }
            };
            var res2 = await _orders.UpdateOneAsync(filter2, update, opts2, ct);
            return (int)res2.ModifiedCount;
        }

        public async Task<bool> DeleteOrderAsync(int orderId, CancellationToken ct = default)
        {
            var orderFilter = Builders<BsonDocument>.Filter.Eq("orderId", orderId);
            var order = await _orders.Find(orderFilter).FirstOrDefaultAsync(ct);
            if (order is null)
                return false;

            if (!order.TryGetValue("orderDetails", out var detailsVal) || detailsVal == BsonNull.Value)
                detailsVal = new BsonArray();

            var orderDetails = detailsVal.AsBsonArray;

            if (orderDetails.Count == 0)
            {
                var delResult = await _orders.DeleteOneAsync(orderFilter, ct);
                return delResult.DeletedCount > 0;
            }

            const int DeallocTypeId = 3;

            var ops = new List<WriteModel<BsonDocument>>(orderDetails.Count);
            foreach (var line in orderDetails)
            {
                var d = line.AsBsonDocument;

                if (!d.TryGetValue("productId", out var productIdVal) ||
                    !d.TryGetValue("quantity", out var qtyVal))
                {
                    continue;
                }

                var productId = productIdVal.ToInt32();
                var quantity = Math.Abs(qtyVal.ToInt32());

                var filter = Builders<BsonDocument>.Filter.Eq("id", productId);

                var txDoc = new BsonDocument
                {
                    { "transactionType", DeallocTypeId },
                    { "transactionCreatedDate", DateTime.UtcNow },
                    { "productId", productId },
                    { "quantity", quantity },
                    { "orderId", orderId },
                    { "comments", "Deallocation on order delete" }
                };

                var update = Builders<BsonDocument>.Update.Push("inventoryTransactions", txDoc);

                ops.Add(new UpdateOneModel<BsonDocument>(filter, update));
            }

            if (ops.Count > 0)
            {
                await _products.BulkWriteAsync(ops, cancellationToken: ct);
            }

            var result = await _orders.DeleteOneAsync(orderFilter, ct);
            return result.DeletedCount > 0;
        }
    }
}