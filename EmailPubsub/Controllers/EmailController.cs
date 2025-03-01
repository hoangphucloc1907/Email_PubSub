using EmailPubsub.Models;
using EmailPubsub.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace EmailPubsub.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EmailController : ControllerBase
    {
        private readonly PubService _pubService;

        public EmailController(PubService pubService)
        {
            _pubService = pubService;
        }

        [HttpPost("send")]
        public async Task<IActionResult> SendEmail([FromBody] Email emailRequest)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            await _pubService.SendEmailMessage(emailRequest);
            return Ok("Email đã được gửi vào hàng đợi.");
        }
    }
}
