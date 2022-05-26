<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class GetServiceUrlTest extends TestCase
{
    public function testInvalidApiIndexResponseThrowsException(): void
    {
        $fakeClient = $this->createClientWithFakeIndexResponse([]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('API index is missing "services" section');
        $fakeClient->getServiceUrl('dummy');
    }

    public function testInvalidServiceNameThrowsException(): void
    {
        $serviceName = 'dummy';

        $fakeClient = $this->createClientWithFakeIndexResponse([
            'services' => [],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(sprintf('No service with ID "%s" found', $serviceName));
        $fakeClient->getServiceUrl($serviceName);
    }

    public function testServiceWithoutUrlThrowsException(): void
    {
        $serviceName = 'dummy';

        $fakeClient = $this->createClientWithFakeIndexResponse([
            'services' => [
                ['id' => $serviceName, /* no URL */],
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(sprintf('Definition of service "%s" is missing URL', $serviceName));
        $fakeClient->getServiceUrl($serviceName);
    }

    public function testServiceUrlIsReturned(): void
    {
        $serviceName = 'dummy';
        $serviceUrl = 'https://dummy.keboola.com/dummy';

        $fakeClient = $this->createClientWithFakeIndexResponse([
            'services' => [
                ['id' => $serviceName, 'url' => $serviceUrl],
            ],
        ]);

        self::assertSame($serviceUrl, $fakeClient->getServiceUrl($serviceName));
    }

    /**
     * @param mixed $response
     * @return Client|MockObject
     */
    private function createClientWithFakeIndexResponse($response)
    {
        $fakeClient = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['indexAction'])
            ->getMock()
        ;

        $fakeClient->expects(self::once())->method('indexAction')->willReturn($response);

        return $fakeClient;
    }
}
