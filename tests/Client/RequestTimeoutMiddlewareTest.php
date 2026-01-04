<?php

namespace Keboola\UnitTest\Client;

use GuzzleHttp\Psr7\Request;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Client\RequestTimeoutMiddleware;
use PHPUnit\Framework\TestCase;

class RequestTimeoutMiddlewareTest extends TestCase
{
    public function testWillSetDefaultTimeout(): void
    {
        $assertingHandler = function ($request, $options) {
            $this->assertSame(RequestTimeoutMiddleware::REQUEST_TIMEOUT_DEFAULT, $options['timeout']);
        };
        $requestMock = new Request('GET', '/lorem-ipsum');

        $middleware = RequestTimeoutMiddleware::factory()($assertingHandler);

        $middleware($requestMock, []);
    }

    public function testWillOverrideTimeout(): void
    {
        $assertingHandler = function ($request, $options) {
            $this->assertSame(RequestTimeoutMiddleware::REQUEST_TIMEOUT_DEFAULT, $options['timeout']);
        };
        $requestMock = new Request('GET', '/lorem-ipsum');

        $middleware = RequestTimeoutMiddleware::factory()($assertingHandler);

        $middleware($requestMock, ['timeout' => 300]);
    }

    public function testWillSetDeleteTimeout(): void
    {
        $assertingHandler = function ($request, $options) {
            $this->assertSame(RequestTimeoutMiddleware::REQUEST_TIMEOUT_EXTENDED, $options['timeout']);
        };
        $requestMock = new Request('DELETE', '/lorem-ipsum');

        $middleware = RequestTimeoutMiddleware::factory()($assertingHandler);

        $middleware($requestMock, []);
    }

    public function testWillSetManualExtendedTimeout(): void
    {
        $assertingHandler = function ($request, $options) {
            $this->assertSame(RequestTimeoutMiddleware::REQUEST_TIMEOUT_EXTENDED, $options['timeout']);
        };
        $requestMock = new Request('DELETE', '/lorem-ipsum');

        $middleware = RequestTimeoutMiddleware::factory()($assertingHandler);

        $middleware($requestMock, [Client::REQUEST_OPTION_EXTENDED_TIMEOUT => true, 'timeout' => 123]);
    }
}
