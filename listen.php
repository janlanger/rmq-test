<?php declare(strict_types = 1);

use Bunny\Channel;
use Bunny\Client;
use Bunny\Exception\ClientException;
use Bunny\Message as BunnyMessage;
use React\EventLoop\Loop;
use React\Promise\PromiseInterface;
use Tracy\Debugger;
use function React\Async\async;
use function React\Promise\set_rejection_handler;

require __DIR__ . '/vendor/autoload.php';

@mkdir(__DIR__ . '/log');
Debugger::enable(Debugger::Development, __DIR__ . '/log');
Debugger::$logSeverity = E_ALL;

if ($argc !== 5) {
	die("Usage: php script.php <queueName> <host> <user> <password>\n");
}

// Retrieve the arguments.
$queueName = $argv[1];
$host      = $argv[2];
$user      = $argv[3];
$password  = $argv[4];

$client = new Bunny\Client([
	'host' => $host,
	'port' => 5672,
	'vhost' => '/',
	'user' => $user,
	'password' => $password,
	'timeout' => 3,
	'read_write_timeout' => 3,
	'tcp_nodelay' => 1,
	'heartbeat' => 600,
]);

// strict error handling
$previousErrorHandler = set_error_handler(static function (int $severity, string $message, string $file, int $line, array $context = []) use (&$previousErrorHandler): bool {
	$reporting = error_reporting();
	if (($severity & $reporting) !== $severity) {
		return false;
	}
	if (($severity & Debugger::$logSeverity) === $severity) {
		$e = new ErrorException($message, severity: $severity, filename: $file, line: $line);
		@$e->context = $context; // @phpstan-ignore-line property.notFound

		throw $e;
	}

	return $previousErrorHandler !== null ? $previousErrorHandler($severity, $message, $file, $line) : false;
});

Loop::futureTick(async(function () use ($queueName, $client) {
	/** @var Channel $channel */
	$channel = $client->channel();
	$channel->exchangeDeclare($queueName, durable: true);
	$channel->queueDeclare($queueName, durable: true);
	$channel->queueBind($queueName, $queueName, $queueName);

	for ($i = 0; $i < 10; $i++) {
		$channel->publish('test message content', exchange: $queueName, routingKey: $queueName);
	}

	$consumerRouter = new ConsumerRouter($client);
	$consumerRouter->listen($queueName, 10);
}));

class ConsumerRouter
{

	public const string HEADER_DELAY = 'expiration';
	private ?Channel $publishChannel = null;
	private bool $stopping = false;

	public function __construct(
		private Client $client,
	)
	{
	}

	public function listen(string $queueName, int $prefetchCount): void
	{
		set_rejection_handler(static function (Throwable $e): void {
			Debugger::log($e);
		});
		try {
			$channel = $this->client->channel();
			$channel->qos(0, $prefetchCount);
			$channel->consume(async(function (BunnyMessage $bunnyMessage, Channel $channel) use ($queueName): void {
				if ($this->stopping) {
					$this->writeln('Consumer is stopping, quitting...');
					return;
				}

				try {
					try {
						$this->writeln('Message received: -- ' . $bunnyMessage->content);

						$promiseOrNull = $this->doConsume($bunnyMessage, $queueName);
						if ($promiseOrNull instanceof PromiseInterface) {
							\React\Async\await($promiseOrNull);
						}

						$channel->ack($bunnyMessage);
					} catch (Throwable $e) {
						$this->writeln('Message rejected - ' . $e->getMessage());
						Debugger::log($e);
						$channel->reject($bunnyMessage);
						throw $e;
					}
				} catch (Throwable $e) {
					$this->stopConsumer();
					throw $e;
				}
			}), $queueName, concurrency: 1);

			$this->writeln(sprintf('Listening on queue %s', $queueName));
			$this->writeln('Waiting for messages...');
		} catch (Throwable $e) {
			if ($this->client->isConnected()) {
				try {
					$this->client->disconnect();
				} catch (ClientException) {
					// nothing
				}
			}
			Loop::stop();

			throw $e;
		}
	}

	private function writeln(string $message): void
	{
		echo $message . "\n";
	}

	private function stopConsumer(): void
	{
		try {
			$this->writeln('Quitting...');
			$this->stopping = true;
			$this->client->disconnect();
		} catch (Throwable $e) {
			Debugger::log($e);
			$this->writeln('Failed to disconnect from RabbitMQ, stopping consumer anyway');
		}
	}

	private function doConsume(BunnyMessage $bunnyMessage, string $queueName): ?PromiseInterface
	{
		// simulate long asynchronous operation with message publishing
		return \React\Promise\Timer\sleep(10)->then(function () use ($bunnyMessage, $queueName): void {
			$this->writeln($bunnyMessage->content);
			$this->publishMessage($bunnyMessage, $queueName);
		})->then(function () {
			return \React\Promise\Timer\sleep(10);
		});
	}

	private function publishMessage(BunnyMessage $bunnyMessage, string $queueName): void
	{
		if ($this->publishChannel === null) {
			$this->publishChannel = $this->client->channel();
		}
		$this->publishChannel->publish(
			$bunnyMessage->content,
			[],
			$queueName,
			$queueName,
		);
	}

}


