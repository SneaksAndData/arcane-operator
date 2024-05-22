using Arcane.Operator.Models.Commands;

namespace Arcane.Operator.Extensions;

/// <summary>
/// The StreamClassPhase extension methods.
/// </summary>
public static class StreamClassPhaseExtensions
{
    /// <summary>
    /// Returns True if the lifecycle phase is final.
    /// </summary>
    /// <param name="phase">Phase</param>
    /// <returns></returns>
    public static bool IsFinal(this StreamClassPhase phase)
    {
        return phase is StreamClassPhase.FAILED or StreamClassPhase.STOPPED;
    }
}
