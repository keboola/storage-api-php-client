<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Keboola\StorageApi\Downloader\BlobClientFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Psr\Http\Message\RequestInterface;

/**
 * Drives the stalling-blob-server.php fixture: a real HTTP server that announces a 256 KiB blob
 * and sends as much of it as the test asks for, then stalls and closes.
 */
trait StallingBlobServer
{
    /** Shortened stall window, so a stalled transfer does not hold the suite for a minute. */
    private const STALL_WINDOW_SECONDS = 1;

    private const BLOB_SIZE_BYTES = 262144;

    private ?StallingBlobServerProcess $stallingServer = null;

    protected function tearDown(): void
    {
        $this->stallingServer?->stop();
        $this->stallingServer = null;
        parent::tearDown();
    }

    private function startStallingServer(
        int $stallSeconds,
        int $bodyBytes = 65536,
        ?string $attemptLog = null,
    ): int {
        $this->stallingServer = StallingBlobServerProcess::start($stallSeconds, $bodyBytes, $attemptLog);

        return $this->stallingServer->port;
    }

    /**
     * The production download policy would wait out six 60 s stall windows, so the tests assemble
     * their own client with a short window and push the retry middleware they need themselves.
     */
    private function createClientWithShortStallWindow(int $port, bool $clearStreamOption): BlobRestProxy
    {
        $http = BlobClientFactory::downloadTransferOptions();
        $http['read_timeout'] = self::STALL_WINDOW_SECONDS;
        if ($http['curl'] !== []) {
            $http['curl'][CURLOPT_LOW_SPEED_TIME] = self::STALL_WINDOW_SECONDS;
        }

        $client = BlobRestProxy::createBlobService(
            sprintf('BlobEndpoint=http://127.0.0.1:%d;SharedAccessSignature=sv=2020-08-04&sig=stub', $port),
            ['http' => $http],
        );
        if ($clearStreamOption) {
            // a copy of what createDownloadClient() pushes; that the factory really pushes it is
            // what BlobClientFactoryTest::testDownloadClientKeepsTheBodyOffTheStreamHandler() covers
            $client->pushMiddleware(static fn (callable $handler): callable
                => static fn (RequestInterface $request, array $options)
                    => $handler($request, ['stream' => false] + $options));
        }

        return $client;
    }
}
