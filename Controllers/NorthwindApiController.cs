using Microsoft.AspNetCore.Mvc;
using WebApplication11.Interfaces;
using static WebApplication11.Models.Models;

namespace WebApplication11.Controllers
{

    namespace NorthwindCompareApi.Controllers
    {
        [ApiController]
        [Route("api/[controller]")]
        public class NorthwindApiController : ControllerBase
        {
            private readonly IDataProvider _provider;

            public NorthwindApiController(IDataProvider provider)
            {
                _provider = provider;
            }

            [HttpGet("orders/{orderId:int}")]
            public async Task<ActionResult<OrderDto?>> GetOrderWithLines(int orderId, CancellationToken ct)
            {
                var order = await _provider.GetOrderWithLinesAsync(orderId, ct);
                return order is null ? NotFound() : Ok(order);
            }

            [HttpGet("customers/{customerId:int}/orders")]
            public async Task<ActionResult<List<OrderHistoryRow>>> GetCustomerOrderHistory(int customerId, CancellationToken ct = default)
            {
                var rows = await _provider.GetCustomerOrderHistoryAsync(customerId, ct: ct);
                return Ok(rows);
            }

            [HttpGet("products/search")]
            public async Task<ActionResult<List<ProductSearchRow>>> SearchProducts([FromQuery] string? q, CancellationToken ct = default)
            {
                var rows = await _provider.SearchProductsAsync(q, ct: ct);
                return Ok(rows);
            }


            public sealed record CreateOrderRequest(
                int CustomerId,
                int EmployeeId,
                int ShipViaId,
                decimal Freight,
                List<NewOrderItem> Items,
                byte AllocTypeId = 2
            );

            [HttpPost("orders")]
            public async Task<ActionResult<int>> CreateOrder([FromBody] CreateOrderRequest req, CancellationToken ct)
            {
                if (!ModelState.IsValid) return ValidationProblem(ModelState);
                var id = await _provider.CreateOrderWithAllocAsync(
                    req.CustomerId, req.EmployeeId, req.ShipViaId, req.Freight, req.Items, req.AllocTypeId, ct);
                return CreatedAtAction(nameof(GetOrderWithLines), new { orderId = id }, id);
            }


            [HttpGet("sales/daily")]
            public async Task<ActionResult<List<SalesByDay>>> GetDailySales([FromQuery] DateTime fromUtc, [FromQuery] DateTime toUtc,
                CancellationToken ct = default)
            {
                var rows = await _provider.GetDailySalesAsync(fromUtc, toUtc, ct);
                return Ok(rows);
            }

            [HttpGet("products/top")]
            public async Task<ActionResult<List<TopProductRow>>> GetTopProducts([FromQuery] DateTime fromUtc, [FromQuery] DateTime toUtc,
                [FromQuery] int topN = 10,
                CancellationToken ct = default)
            {
                var rows = await _provider.GetTopProductsAsync(fromUtc, toUtc, topN, ct);
                return Ok(rows);
            }

            [HttpGet("inventory/reorder-suggestions")]
            public async Task<ActionResult<List<ReorderSuggestionRow>>> GetReorderSuggestions(CancellationToken ct)
                => Ok(await _provider.GetReorderSuggestionsAsync(ct));

            public sealed record BumpLineRequest(int OrderDetailId,
                                                 double Delta);

            [HttpPatch("order-lines/bump")]
            public async Task<ActionResult<int>> BumpOrderLineQuantity( [FromBody] BumpLineRequest req, CancellationToken ct)
            {
                if (!ModelState.IsValid) return ValidationProblem(ModelState);
                var rows = await _provider.BumpOrderLineQuantityAsync(req.OrderDetailId, req.Delta, ct);
                return Ok(rows);
            }

            [HttpGet("provider")]
            public ActionResult<string> Provider()
                => Ok(_provider.GetType().Name);

            [HttpDelete("orders/{orderId:int}")]
            public async Task<IActionResult> DeleteOrder(int orderId, CancellationToken ct)
            {
                var ok = await _provider.DeleteOrderAsync(orderId, ct);
                if (!ok) return NotFound();
                return NoContent();
            }
        }
    }
}