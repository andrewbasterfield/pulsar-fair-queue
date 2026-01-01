TENANT="public"
NAMESPACE="$TENANT/queues"
TOPIC_PREFIX="persistent://$NAMESPACE"
TOPIC_NOISY="$TOPIC_PREFIX/queue-class-noisy"
TOPIC_QUIET="$TOPIC_PREFIX/queue-class-quiet"
TOPIC_REGEX="$TOPIC_PREFIX/queue-class-.*"

SUBSCRIPTION_NAME="fair-subscription"

EXEC="docker-compose exec -T broker"
RUN="docker-compose run -T --rm --no-deps pulsar-init"
PULSAR_CLIENT="$EXEC bin/pulsar-client --url pulsar://broker:6650"
PULSAR_ADMIN="$EXEC bin/pulsar-admin --admin-url http://broker:8080"
PULSAR_CLIENT_RUN="$RUN bin/pulsar-client --url pulsar://broker:6650"
