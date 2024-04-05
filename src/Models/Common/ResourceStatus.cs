namespace Arcane.Operator.Models.Common;

/// <summary>
/// Represents stream status badge for Lens app.
/// </summary>
public enum ResourceStatus
{
    /// <summary>
    /// The stream is in a ready state.
    /// </summary>
    READY,

    /// <summary>
    /// The stream is in an error state.
    /// </summary>
    ERROR,

    /// <summary>
    /// The stream is in a warning state.
    /// </summary>
    WARNING
}
