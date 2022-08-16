<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */
namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\IndexOptions;
use Keboola\Test\StorageApiTestCase;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;

class LoggingTest extends StorageApiTestCase
{

    public function testLogger(): void
    {
        $logger = $this->getMockBuilder(NullLogger::class)
            ->getMock();

        $logger->expects($this->once())
            ->method('log');

        $client = $this->getClient([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'logger' => $logger,
        ]);
        $client->verifyToken();
    }

    public function testAwsLogger(): void
    {
        $logger = $this->getMockBuilder(NullLogger::class)
            ->getMock();

        $logger->expects($this->atLeastOnce())
            ->method('log')
            ->with($this->callback(function ($level) {
                if ($level === LogLevel::INFO) {
                    return false;
                }
                return true;
            }));

        $client = $this->getClient([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'logger' => $logger,
            'awsDebug' => true,
            'backoffMaxTries' => 1,
        ]);
        $options = new \Keboola\StorageApi\Options\FileUploadOptions();
        $client->uploadFile(__DIR__ . '/../_data/files.upload.txt', $options);
    }

    public function testRequestLog(): void
    {
        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $client = new Client([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'userAgent' => 'Client Testing',
            'logger' => $logger,
            'backoffMaxTries' => 1,
        ]);

        $client->indexAction((new IndexOptions())->setExclude(['components']));

        self::assertTrue($logsHandler->hasDebug(
            'GET https://connection.keboola.com/v2/storage/?exclude=components 200'
        ));
    }

    public function testFailedRequestLog(): void
    {
        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $client = new Client([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'userAgent' => 'Client Testing',
            'logger' => $logger,
            'backoffMaxTries' => 1,
        ]);

        try {
            $client->apiGet('invalid/url');
            self::fail('ClientException was expected');
        } catch (ClientException $e) {
            // ignore exception, we want to check logs
        }

        self::assertTrue($logsHandler->hasError(
            'GET https://connection.keboola.com/v2/storage/invalid/url 404 "{\"error\":\"resource not found\"}"'
        ));
    }
}
