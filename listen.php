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

Loop::futureTick(async(function () use ($client, $queueName) {
	/** @var Channel $channel */
	$channel = $client->channel();
	$channel->exchangeDeclare('test', durable: true);
	$channel->queueDeclare($queueName, durable: true);
	$channel->queueBind($queueName, 'test', $queueName);
	
	for ($i = 0; $i < 10; $i++) {
		$channel->publish('test message content', exchange: 'test', routingKey: $queueName);
	}
	
	$consumerRouter = new ConsumerRouter($client);
	$consumerRouter->listen($queueName, 10);
}));

class ConsumerRouter
{

	public const string HEADER_DELAY = 'expiration';

	/** @var PromiseInterface<void>|null */
	private ?PromiseInterface $stoppingPromise = null;
	private bool $messageIsConsuming = false;

	public function __construct(
		private Client $client,
	)
	{
	}

	/**
	 * @param Closure(string, int=): void $onDebugMessage
	 */
	public function listen(string $queueName, int $prefetchCount): void
	{
		set_rejection_handler(static function (Throwable $e): void {
			Debugger::log($e);
		});
		try {
			$channel = $this->client->channel();
			$channel->qos(0, $prefetchCount);
			$publishChannel = null;
			$channel->consume(async(function (BunnyMessage $bunnyMessage, Channel $channel) use (&$publishChannel, $queueName): void {

				try {
					try {
						$this->writeln('Message received: -- ' . $bunnyMessage->content);
						if ($this->messageIsConsuming) {
							throw new Exception('Some message is alredy in processing.');
						}
						$this->messageIsConsuming = true;
						$this->writeln('Inside transaction');
						sleep(1);
						sleep(1);

						$this->writeln('Publishing message');
						if ($publishChannel === null) {
							$publishChannel = $this->client->channel();
						}
						$publishChannel->publish(
							$bunnyMessage->content,
							[],
							$queueName,
							$queueName,
						);
						$this->writeln('Message published');

						sleep(1);
						sleep(1);
						sleep(1);
						$this->writeln('Done consuming');
						$this->messageIsConsuming = false;
						$this->writeln(" <info>[Done]</info>\n");
						$channel->ack($bunnyMessage);
					} catch (Throwable $e) {
						$channel->reject($bunnyMessage);
						$this->messageIsConsuming = false;
						$this->writeln('BOOM - ' . $e->getMessage() . "\n");

						throw $e;
					}
				} catch (Throwable $e) {
					$this->stopConsumer();
					throw $e;
				}
			}), $queueName);

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

		Loop::run();
	}

	private function writeln(string $message): void
	{
		echo $message . "\n";
	}

	private function stopConsumer(): void
	{
		if ($this->stoppingPromise !== null) {
			return;
		}

		$this->stoppingPromise = async(function (): void {
			try {
				$this->writeln('Quitting...');
				$this->client->disconnect();
			} catch (Throwable $e) {
				Debugger::log($e);
				$this->writeln('Failed to disconnect from RabbitMQ, stopping consumer anyway');
			} finally {
				Loop::stop();
			}
		})();
	}

}


