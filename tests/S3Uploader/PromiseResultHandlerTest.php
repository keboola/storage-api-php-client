<?php

namespace Keboola\Test\S3Uploader;

use Aws\Command;
use Aws\Exception\AwsException;
use Aws\Multipart\UploadState;
use Aws\S3\Exception\S3MultipartUploadException;
use Keboola\StorageApi\S3Uploader\PromiseResultHandler;
use Keboola\Test\StorageApiTestCase;

class PromiseResultHandlerTest extends StorageApiTestCase
{
    public function testGetRejected(): void
    {
        $results = [
            'filepath1' => [
                'state' => 'fulfilled',
            ],
            'filepath2' => [
                'state' => 'rejected',
                'reason' => new S3MultipartUploadException(
                    new UploadState([]),
                    new AwsException('DummyAwsException', new Command('DummyCommand', ['Key' => 'DummyKey'])),
                ),
            ],
        ];
        $rejected = PromiseResultHandler::getRejected($results);
        $this->assertCount(1, $rejected);
        $this->assertArrayHasKey('filepath2', $rejected);
        $this->assertEquals('DummyKey', $rejected['filepath2']->getKey());
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
            PromiseResultHandler::getRejected($results);
            $this->fail('Exception not caught.');
        } catch (\UnexpectedValueException $e) {
            $this->assertEquals('Not an instance of S3MultipartUploadException', $e->getMessage());
        }
    }
}
