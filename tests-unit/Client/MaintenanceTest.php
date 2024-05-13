<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Client;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use PHPUnit\Framework\TestCase;

class MaintenanceTest extends TestCase
{
    public function testMaintenanceResponseIsRetriedByDefault(): void
    {
        $mockHandler = new MockHandler([
            new Response(503),
            new Response(200, [], 'ok'),
        ]);

        $client = new Client([
            'token' => '123-foo',
            'url' => 'http://example.com',
            'handler' => $mockHandler,
        ]);

        $response = $client->apiGet('/');
        self::assertSame('ok', $response);
    }

    public function testMaintenanceRetryCanBeDisabled(): void
    {
        $mockHandler = new MockHandler([
            new Response(503),
            new Response(200, [], 'ok'),
        ]);

        $client = new Client([
            'retryOnMaintenance' => false,
            'token' => '123-foo',
            'url' => 'http://example.com',
            'handler' => $mockHandler,
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Maintenance');

        $client->apiGet('/');
    }
}
