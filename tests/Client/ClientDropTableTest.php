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

final class ClientDropTableTest extends TestCase
{
    /**
     * @return iterable<string, array{0: array<string, bool>, 1: string}>
     */
    public static function optionsProvider(): iterable
    {
        yield 'no options' => [[], 'async=1'];
        yield 'force only' => [['force' => true], 'force=1&async=1'];
        yield 'force with async' => [['force' => true, 'async' => true], 'force=1&async=1'];
        yield 'explicit sync' => [['force' => true, 'async' => false], 'force=1&async=0'];
        yield 'sync without force' => [['async' => false], 'async=0'];
    }

    /**
     * @param array<string, bool> $options
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('optionsProvider')]
    public function testDropTableQueryParameters(array $options, string $expectedQuery): void
    {
        /** @var array<int, array{request: Request}> $historyContainer */
        $historyContainer = [];
        $mock = new MockHandler([
            new Response(204),
        ]);
        $stack = HandlerStack::create($mock);
        $stack->push(Middleware::history($historyContainer));
        $client = new Client([
            'token' => 'token',
            'url' => 'https://connection.example',
            'handler' => $stack,
        ]);

        $client->dropTable('in.c-main.orders', $options);

        /** @var array<int, array{request: Request}> $history */
        $history = $historyContainer;
        $this->assertCount(1, $history);
        $request = $history[0]['request'];
        $this->assertSame('DELETE', $request->getMethod());
        $this->assertSame('/v2/storage/tables/in.c-main.orders', $request->getUri()->getPath());
        $this->assertSame($expectedQuery, $request->getUri()->getQuery());
    }

    public function testDropTableAwaitsTheAsyncJob(): void
    {
        /** @var array<int, array{request: Request}> $historyContainer */
        $historyContainer = [];
        $mock = new MockHandler([
            new Response(202, ['Content-type' => 'application/json'], (string) json_encode([
                'id' => 123,
                'operationName' => 'deleteTable',
            ])),
            new Response(200, ['Content-type' => 'application/json'], (string) json_encode([
                'id' => 123,
                'status' => 'success',
                'results' => [],
            ])),
        ]);
        $stack = HandlerStack::create($mock);
        $stack->push(Middleware::history($historyContainer));
        $client = new Client([
            'token' => 'token',
            'url' => 'https://connection.example',
            'handler' => $stack,
        ]);

        $client->dropTable('in.c-main.orders', ['force' => true]);

        /** @var array<int, array{request: Request}> $history */
        $history = $historyContainer;
        $this->assertCount(2, $history);
        $jobRequest = $history[1]['request'];
        $this->assertSame('GET', $jobRequest->getMethod());
        $this->assertSame('/v2/storage/jobs/123', $jobRequest->getUri()->getPath());
    }
}
