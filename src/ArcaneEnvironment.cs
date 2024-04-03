namespace Arcane.Operator;

public static class ArcaneEnvironment
{
    public static string DefaultVarPrefix => $"{nameof(Arcane).ToUpper()}__";

    public static string GetEnvironmentVariableName(this string name)
    {
        return $"{DefaultVarPrefix}{name}".ToUpperInvariant();
    }
}
