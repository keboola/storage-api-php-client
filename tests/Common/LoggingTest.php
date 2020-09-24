<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */
namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;

class LoggingTest extends StorageApiTestCase
{

    public function testLogger()
    {
        $logger = $this->getMockBuilder(NullLogger::class)
            ->getMock();

        $logger->expects($this->once())
            ->method('log');

        $client = $this->getClient(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'logger' => $logger,
        ));
        $client->verifyToken();
    }

    public function testAwsLogger()
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

        $client = $this->getClient(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'logger' => $logger,
            'awsDebug' => true,
            'backoffMaxTries' => 1,
        ));
        $options = new \Keboola\StorageApi\Options\FileUploadOptions();
        $client->uploadFile(__DIR__ . '/../_data/files.upload.txt', $options);
    }
}
