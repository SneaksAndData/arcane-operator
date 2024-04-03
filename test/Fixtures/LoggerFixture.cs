using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Tests.Fixtures;

public class LoggerFixture
{
    public LoggerFixture()
    {
        this.Factory = LoggerFactory.Create(conf => conf.AddConsole());
    }

    public ILoggerFactory Factory { get; }
}
