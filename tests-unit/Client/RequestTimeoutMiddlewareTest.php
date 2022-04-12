<?php

namespace Keboola\UnitTest\Client;

use GuzzleHttp\Psr7\Request;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Client\RequestTimeoutMiddleware;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class RequestTimeoutMiddlewareTest extends TestCase
{
    /**
     * @return void
     */
    public function testWillSetDefaultTimeout()
    {
        $assertingHandler = function ($request, $options) {
            $this->assertSame(60, $options['timeout']);
        };
        $requestMock = new Request('GET', '/lorem-ipsum');

        $middleware = RequestTimeoutMiddleware::factory(new NullLogger())($assertingHandler);

        $middleware($requestMock, []);
    }

    /**
     * @return void
     */
    public function testWillOverrideTimeout()
    {
        $assertingHandler = function ($request, $options) {
            $this->assertSame(60, $options['timeout']);
        };
        $requestMock = new Request('GET', '/lorem-ipsum');

        $middleware = RequestTimeoutMiddleware::factory(new NullLogger())($assertingHandler);

        $middleware($requestMock, ['timeout' => 300]);
    }

    /**
     * @return void
     */
    public function testWillSetDeleteTimeout()
    {
        $assertingHandler = function ($request, $options) {
            $this->assertSame(7200, $options['timeout']);
        };
        $requestMock = new Request('DELETE', '/lorem-ipsum');

        $middleware = RequestTimeoutMiddleware::factory(new NullLogger())($assertingHandler);

        $middleware($requestMock, []);
    }

    /**
     * @return void
     */
    public function testWillSetManualExtenededTimeout()
    {
        $assertingHandler = function ($request, $options) {
            $this->assertSame(7200, $options['timeout']);
        };
        $requestMock = new Request('DELETE', '/lorem-ipsum');

        $middleware = RequestTimeoutMiddleware::factory(new NullLogger())($assertingHandler);

        $middleware($requestMock, [Client::REQUEST_OPTION_EXTENDED_TIMEOUT => true, 'timeout' => 123]);
    }
}
