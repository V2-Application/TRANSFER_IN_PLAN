using Microsoft.AspNetCore.Mvc;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsStatusController : Controller
{
    private readonly ILogger<ArsStatusController> _logger;
    private readonly IHttpClientFactory _httpFactory;
    private const string ARS_BASE = "https://ars-v2retail-api.azurewebsites.net";

    public ArsStatusController(ILogger<ArsStatusController> logger, IHttpClientFactory httpFactory)
    {
        _logger = logger;
        _httpFactory = httpFactory;
    }

    public IActionResult Index() => View();

    [HttpGet]
    public async Task<IActionResult> CheckHealth()
    {
        try
        {
            var client = _httpFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(10);
            var resp = await client.GetAsync($"{ARS_BASE}/docs");
            return Json(new
            {
                online = resp.IsSuccessStatusCode,
                statusCode = (int)resp.StatusCode,
                url = ARS_BASE
            });
        }
        catch (Exception ex)
        {
            return Json(new { online = false, statusCode = 0, url = ARS_BASE, error = ex.Message });
        }
    }
}
