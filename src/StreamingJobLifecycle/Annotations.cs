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

    public const string CONFIGURATION_CHECKSUM_ANNOTATION_KEY = "arcane/configuration-checksum";

    public const string ARCANE_STREAM_API_GROUP = "arcane.sneaksanddata.com/stream/api-group";
    public const string ARCANE_STREAM_API_VERSION = "arcane.sneaksanddata.com/stream/api-version";
    public const string ARCANE_STREAM_API_PLURAL_NAME = "arcane.sneaksanddata.com/stream/api-plural-name";

}
