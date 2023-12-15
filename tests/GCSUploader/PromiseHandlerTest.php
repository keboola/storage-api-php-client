<?php

namespace Keboola\Test\GCSUploader;

use Google\Cloud\Core\Exception\ServiceException;
use Keboola\StorageApi\GCSUploader\PromiseHandler;
use Keboola\Test\StorageApiTestCase;

class PromiseHandlerTest extends StorageApiTestCase
{
    public function testGetRejected(): void
    {
        $results = [
            'filepath1' => [
                'state' => 'fulfilled',
            ],
            'filepath2' => [
                'state' => 'rejected',
                'reason' => new ServiceException(
                    'dummyMessage',
                    0,
                    new \Exception('dummyReason'),
                ),
            ],
        ];
        $rejected = PromiseHandler::getRejected($results);
        self::assertCount(1, $rejected);
        self::assertArrayHasKey('filepath2', $rejected);
    }

    public function testGetRejectedException(): void
    {
        $results = [
            'filepath' => [
                'state' => 'rejected',
                'reason' => new \Exception(),
            ],
        ];
        try {
            PromiseHandler::getRejected($results);
            self::fail('Exception not caught.');
        } catch (\UnexpectedValueException $e) {
            self::assertEquals('Not an instance of Google Cloud ServiceException', $e->getMessage());
        }
    }
}
