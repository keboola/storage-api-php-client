<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Keboola\StorageApi\Client;

use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\Client\LogMessageFormatter;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class LogMessageFormatterTest extends TestCase
{
    /** @dataProvider provideFormatterTestData */
    public function testFormatter(
        RequestInterface $request,
        ?ResponseInterface $response,
        ?Throwable $error,
        string $expectedMessage
    ): void {
        $formatter = new LogMessageFormatter();
        $message = $formatter->format($request, $response, $error);

        self::assertSame($expectedMessage, $message);
    }

    public function provideFormatterTestData(): iterable
    {
        yield 'request with complex URL' => [
            'request' => new Request(
                'GET',
                'http://example.com/path?query=value#fragment',
                [],
                'request body',
            ),
            'response' => new Response(
                201,
                [],
                'response body',
            ),
            'error' => null,
            'message' => 'GET http://example.com/path?query=value#fragment 201',
        ];

        yield 'POST request' => [
            'request' => new Request(
                'POST',
                'http://example.com/path',
                [],
                'request body',
            ),
            'response' => new Response(
                201,
                [],
                'response body',
            ),
            'error' => null,
            'message' => 'POST http://example.com/path 201',
        ];

        yield 'failed request without error' => [
            'request' => new Request(
                'GET',
                'http://example.com/path',
                [],
                'request body',
                '1.0',
            ),
            'response' => new Response(
                500,
                [],
                'response body',
            ),
            'error' => null,
            'message' => 'GET http://example.com/path 500',
        ];

        yield 'failed request' => [
            'request' => new Request(
                'GET',
                'http://example.com/path',
                [],
                'request body',
                '1.0',
            ),
            'response' => new Response(
                400,
                [],
                'response',
            ),
            'error' => new ClientException('invalid request', new Request('GET', ''), new Response()),
            'message' => 'GET http://example.com/path 400 "response"',
        ];

        $longResponse = str_repeat('X', 4*1024*1024);
        yield 'failed request with long response' => [
            'request' => new Request(
                'GET',
                'http://example.com/path',
                [],
                'request body',
                '1.0',
            ),
            'response' => new Response(
                400,
                [],
                $longResponse,
            ),
            'error' => new ClientException('invalid request', new Request('GET', ''), new Response()),
            'message' => 'GET http://example.com/path 400 "'.$longResponse.'"',
        ];

        yield 'failed request with multi-line response body' => [
            'request' => new Request(
                'GET',
                'http://example.com/path',
                [],
                'request body',
                '1.0',
            ),
            'response' => new Response(
                400,
                [],
                '{
    "hello": "world"
}
',
            ),
            'error' => new ClientException('invalid request', new Request('GET', ''), new Response()),
            'message' => 'GET http://example.com/path 400 "{\n    \"hello\": \"world\"\n}\n"',
        ];

        yield 'error without response' => [
            'request' => new Request(
                'GET',
                'http://example.com/path',
                [],
                'request body',
                '1.0',
            ),
            'response' => null,
            'error' => new ConnectException('failed to cnonect', new Request('GET', '')),
            'message' => 'GET http://example.com/path NULL NULL',
        ];

        yield 'error with success response' => [
            'request' => new Request(
                'GET',
                'http://example.com/path',
                [],
                'request body',
                '1.0',
            ),
            'response' => new Response(
                201,
                [],
                'response',
            ),
            'error' => new ClientException('something has failed', new Request('GET', ''), new Response()),
            'message' => 'GET http://example.com/path 201 "response"',
        ];
    }
}
