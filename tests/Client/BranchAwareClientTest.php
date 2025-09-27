<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Client;

use Generator;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\StorageApi\BranchAwareClient;
use PHPUnit\Framework\TestCase;

class BranchAwareClientTest extends TestCase
{
    public function testGetBranchId(): void
    {
        $branchId = 'branch-id';
        $client = new BranchAwareClient($branchId, ['token' => 'token', 'url' => 'url']);
        self::assertEquals($branchId, $client->getCurrentBranchId());
    }

    public function testGetBranchIdWithEmptyBranchId(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Branch "" is not valid.');
        new BranchAwareClient('', ['token' => 'token', 'url' => 'url']);
    }

    public static function endpointsProvider(): Generator
    {
        yield 'listJobs' => [
            'methodCall' => ['listJobs'],
            'httpMethod' => 'GET',
            'expectedPath' => 'v2/storage/jobs',
        ];
        yield 'getJob' => [
            'methodCall' => ['getJob', [1]],
            'httpMethod' => 'GET',
            'expectedPath' => 'v2/storage/jobs/1',
        ];
        yield 'getSnapshot' => [
            'methodCall' => ['getSnapshot', [1]],
            'httpMethod' => 'GET',
            'expectedPath' => 'v2/storage/snapshots/1',
        ];
        yield 'deleteSnapshot' => [
            'methodCall' => ['deleteSnapshot', [1]],
            'httpMethod' => 'DELETE',
            'expectedPath' => 'v2/storage/snapshots/1',
        ];
        yield 'listTriggers' => [
            'methodCall' => ['listTriggers'],
            'httpMethod' => 'GET',
            'expectedPath' => 'v2/storage/triggers/',
        ];
        yield 'createTrigger' => [
            'methodCall' => ['createTrigger', ['foo', ['bar']]],
            'httpMethod' => 'POST',
            'expectedPath' => 'v2/storage/triggers/',
        ];
        yield 'updateTrigger' => [
            'methodCall' => ['updateTrigger', ['foo', ['bar']]],
            'httpMethod' => 'PUT',
            'expectedPath' => 'v2/storage/triggers/foo/',
        ];
        yield 'getTrigger' => [
            'methodCall' => ['getTrigger', [1]],
            'httpMethod' => 'GET',
            'expectedPath' => 'v2/storage/triggers/1/',
        ];
        yield 'deleteTrigger' => [
            'methodCall' => ['deleteTrigger', [1]],
            'httpMethod' => 'DELETE',
            'expectedPath' => 'v2/storage/triggers/1/',
        ];
        yield 'generateId' => [
            'methodCall' => ['generateId'],
            'httpMethod' => 'POST',
            'expectedPath' => 'v2/storage/tickets',
            'result' => ['id' => '123'],
        ];
        yield 'generateRunId' => [
            'methodCall' => ['generateRunId'],
            'httpMethod' => 'POST',
            'expectedPath' => 'v2/storage/tickets',
            'result' => ['id' => '123'],
        ];
        yield 'setAliasTableFilter' => [
            'methodCall' => ['setAliasTableFilter', ['foo', ['bar']]],
            'httpMethod' => 'POST',
            'expectedPath' => 'v2/storage/tables/foo/alias-filter',
        ];
        yield 'removeAliasTableFilter' => [
            'methodCall' => ['removeAliasTableFilter', ['foo']],
            'httpMethod' => 'DELETE',
            'expectedPath' => 'v2/storage/tables/foo/alias-filter',
        ];
        yield 'createAliasTable' => [
            'methodCall' => ['createAliasTable', ['foo', ['bar']]],
            'httpMethod' => 'POST',
            'expectedPath' => 'v2/storage/buckets/foo/table-aliases',
            'result' => ['id' => '123'],
        ];
        yield 'test other' => [
            'methodCall' => ['listTableEvents', ['foo', ['bar']]],
            'httpMethod' => 'GET',
            'expectedPath' => 'v2/storage/branch/123/tables/foo/events',
        ];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('endpointsProvider')]
    public function testBranchedAwareEndpointsCall(
        array $methodCall,
        string $httpMethod,
        string $expectedPath,
        array $result = [],
    ): void {
        $historyContainer = [];
        $historyMiddleware = Middleware::history($historyContainer);
        $mock = new MockHandler([
            new Response(200, ['Content-type' => 'application/json'], (string) json_encode($result)),
        ]);
        $stack = HandlerStack::create($mock);
        $stack->push($historyMiddleware);
        $client = new BranchAwareClient(123, ['token' => 'token', 'url' => 'url', 'handler' => $stack]);
        $method = $methodCall[0];
        if (array_key_exists(1, $methodCall)) {
            $client->$method(...$methodCall[1]);
        } else {
            $client->$method();
        }
        $this->assertNotEmpty($historyContainer, 'No requests were captured.');
        $this->assertCount(1, $historyContainer, 'Expected exactly one request.');
        /** @var Request $request */
        $request = $historyContainer[0]['request'];
        self::assertEquals($httpMethod, $request->getMethod());
        self::assertEquals($expectedPath, $request->getUri()->getPath());
    }
}
