using Microsoft.AspNetCore.Mvc;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class HomeController : Controller
{
    private readonly PlanService _planService;
    private readonly ILogger<HomeController> _logger;

    public HomeController(PlanService planService, ILogger<HomeController> logger)
    {
        _planService = planService;
        _logger = logger;
    }

    public async Task<IActionResult> Index()
    {
        try
        {
            var dashboardData = await _planService.GetDashboardData();
            return View(dashboardData);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error loading dashboard: {ex.Message}");
            return View(new Models.DashboardViewModel());
        }
    }

    public IActionResult Privacy()
    {
        return View();
    }

    [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
    public IActionResult Error()
    {
        return View();
    }
}
