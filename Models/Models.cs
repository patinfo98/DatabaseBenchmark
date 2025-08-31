namespace WebApplication11.Models
{
    public class Models
    {
        public sealed record OrderDto(
            int OrderId, int CustomerId, int EmployeeId, DateTime OrderDate,
            int ShipVia, decimal Freight,
            List<OrderLineDto> Lines);

        public sealed record OrderLineDto(
            int ProductId, decimal UnitPrice, double Quantity, double Discount, string ProductName);

        public sealed record OrderHistoryRow(int OrderId, DateTime OrderDate, double OrderTotal);

        public sealed record ProductSearchRow(int Id, string ProductName, decimal ListPrice, int? MinimumReorder);

        public sealed record SalesByDay(DateTime Day, double Sales);

        public sealed record TopProductRow(int ProductId, string ProductName, string Category, double Revenue);

        public sealed record CohortRow(DateTime CohortMonth, DateTime OrderMonth, int ActiveCustomers);

        public sealed record ReorderSuggestionRow(
            int ProductId, string ProductName, int? OnHand, int? TargetLevel, short? MinimumReorder, int Last30dOut);

        public sealed record OrderNoteRow(int OrderId, DateTime OrderDate, string? Notes);

        public sealed record NewOrderItem(int ProductID, double UnitPrice, float Discount, float Quantity);
    }
}