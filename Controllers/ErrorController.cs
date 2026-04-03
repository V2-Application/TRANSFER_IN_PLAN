using Microsoft.AspNetCore.Mvc;

namespace TRANSFER_IN_PLAN.Controllers;

[ApiExplorerSettings(IgnoreApi = true)]
public class ErrorController : Controller
{
    public IActionResult Index(int statusCode = 500)
    {
        var reexecuteRoute = new { controller = "Error", action = "Index" };

        switch (statusCode)
        {
            case 404:
                ViewBag.Title = "Page Not Found";
                ViewBag.Message = "The requested page could not be found.";
                ViewBag.ErrorCode = 404;
                break;
            case 500:
                ViewBag.Title = "Server Error";
                ViewBag.Message = "An unexpected error occurred while processing your request.";
                ViewBag.ErrorCode = 500;
                break;
            case 403:
                ViewBag.Title = "Access Forbidden";
                ViewBag.Message = "You do not have permission to access this resource.";
                ViewBag.ErrorCode = 403;
                break;
            default:
                ViewBag.Title = "Error";
                ViewBag.Message = "An error occurred.";
                ViewBag.ErrorCode = statusCode;
                break;
        }

        return View();
    }
}
