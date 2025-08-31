using static WebApplication11.Models.Models;

namespace WebApplication11.Interfaces
{
    public interface IDataProvider
    {
        string GetProvider();

        Task<OrderDto?> GetOrderWithLinesAsync(int orderId, CancellationToken ct = default);

        Task<List<OrderHistoryRow>> GetCustomerOrderHistoryAsync(
            int customerId, int pageSize = 50, CancellationToken ct = default);

        Task<List<ProductSearchRow>> SearchProductsAsync(string? search, int pageSize = 50, CancellationToken ct = default);

        Task<int> CreateOrderWithAllocAsync(
            int customerId, int employeeId, int shipViaId, decimal freight,
            IEnumerable<NewOrderItem> items, byte allocTypeId = 2,
            CancellationToken ct = default);

        Task<List<SalesByDay>> GetDailySalesAsync(DateTime fromUtc, DateTime toUtc, CancellationToken ct = default);

        Task<List<TopProductRow>> GetTopProductsAsync(DateTime fromUtc, DateTime toUtc, int topN, CancellationToken ct = default);

        Task<List<ReorderSuggestionRow>> GetReorderSuggestionsAsync(CancellationToken ct = default);

        Task<int> BumpOrderLineQuantityAsync(int orderDetailId, double delta, CancellationToken ct = default);

        Task<bool> DeleteOrderAsync(int orderId, CancellationToken ct = default);
    }
}
