namespace Arcane.Operator.StreamingJobLifecycle;

public static class Annotations
{
    public const string STATE_ANNOTATION_KEY = "arcane/state";
    public const string TERMINATING_STATE_ANNOTATION_VALUE = "terminating";
    public const string TERMINATE_REQUESTED_STATE_ANNOTATION_VALUE = "terminate-requested";
    public const string SUSPENDED_STATE_ANNOTATION_VALUE = "suspended";
    public const string CRASH_LOOP_STATE_ANNOTATION_VALUE = "crash-loop";
    public const string RESTARTING_STATE_ANNOTATION_VALUE = "restart-requested";
    public const string RELOADING_STATE_ANNOTATION_VALUE = "reload-requested";
    public const string SCHEMA_MISMATCH_STATE_ANNOTATION_VALUE = "schema-mismatch";

    public const string CONFIGURATION_CHECKSUM_ANNOTATION_KEY = "stream.arcane.sneaksanddata.com/configuration-checksum";
    public const string ARCANE_STREAM_API_GROUP = "stream.arcane.sneaksanddata.com/api-group";
    public const string ARCANE_STREAM_API_VERSION = "stream.arcane.sneaksanddata.com/api-version";
    public const string ARCANE_STREAM_API_PLURAL_NAME = "stream.arcane.sneaksanddata.com/api-plural-name";

}
