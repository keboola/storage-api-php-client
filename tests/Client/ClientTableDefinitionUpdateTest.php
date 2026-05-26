<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Client;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

final class ClientTableDefinitionUpdateTest extends TestCase
{
    public function testUpdateTableDefinitionReturnsCompletedJobResult(): void
    {
        /** @var array<int, array{request: Request}> $historyContainer */
        $historyContainer = [];
        $historyMiddleware = Middleware::history($historyContainer);
        $mock = new MockHandler([
            new Response(202, ['Content-type' => 'application/json'], (string) json_encode([
                'id' => 123,
                'operationName' => 'tableDefinitionUpdate',
            ])),
            new Response(200, ['Content-type' => 'application/json'], (string) json_encode([
                'id' => 123,
                'status' => 'success',
                'results' => [
                    'id' => 'in.c-main.orders',
                    'description' => 'Orders table',
                ],
            ])),
        ]);
        $stack = HandlerStack::create($mock);
        $stack->push($historyMiddleware);
        $client = new Client([
            'token' => 'token',
            'url' => 'https://connection.example',
            'handler' => $stack,
        ]);

        $job = $client->updateTableDefinition('in.c-main.orders', [
            'description' => 'Orders table',
        ]);

        $this->assertSame([
            'id' => 'in.c-main.orders',
            'description' => 'Orders table',
        ], $job);
        $this->assertIsArray($historyContainer);
        $this->assertCount(2, $historyContainer);
        /** @var Request $request */
        $request = $historyContainer[0]['request'];
        $this->assertSame('PUT', $request->getMethod());
        $this->assertSame('/v2/storage/tables/in.c-main.orders/definition', $request->getUri()->getPath());
        $this->assertSame(
            '{"description":"Orders table"}',
            (string) $request->getBody(),
        );

        /** @var Request $jobRequest */
        $jobRequest = $historyContainer[1]['request'];
        $this->assertSame('GET', $jobRequest->getMethod());
        $this->assertSame('/v2/storage/jobs/123', $jobRequest->getUri()->getPath());
    }
}
