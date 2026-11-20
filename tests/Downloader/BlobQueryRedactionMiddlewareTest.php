<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Closure;
use Generator;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Exception\ServerException;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\ABSUploader;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Downloader\BlobQueryRedactionMiddleware;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\CommitBlobBlocksOptions;
use MicrosoftAzure\Storage\Blob\Models\CreateBlockBlobOptions;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use RuntimeException;
use Throwable;

/**
 * Security guard: no failed Azure blob request may hand the SAS to its caller.
 */
class BlobQueryRedactionMiddlewareTest extends TestCase
{
    private const SIGNATURE = 'LEAK%2FMARKER%3D';

    private const CONNECTION_STRING = 'BlobEndpoint=http://127.0.0.1:1;SharedAccessSignature=sv=2020-08-04&sp=rl&sig='
        . self::SIGNATURE;

    /**
     * @param Closure(BlobRestProxy): mixed $operation
     */
    #[DataProvider('provideFailingOperations')]
    public function testAFailedRequestDoesNotCarryTheSas(Closure $operation, bool $clearStreamOption): void
    {
        // no retry middleware: the factories' six attempts would only slow the same failure down
        $client = BlobRestProxy::createBlobService(self::CONNECTION_STRING, ['http' => ['connect_timeout' => 1]]);
        if ($clearStreamOption) {
            $client->pushMiddleware(static fn (callable $handler): callable
                => static fn (RequestInterface $request, array $options)
                    => $handler($request, ['stream' => false] + $options));
        }
        $client->pushMiddleware(new BlobQueryRedactionMiddleware());

        try {
            $operation($client);
            self::fail('The request to a closed port must fail.');
        } catch (ConnectException $e) {
            self::assertCarriesNoSas($e);
            self::assertSame('/container/blob', $e->getRequest()->getUri()->getPath());
        }
    }

    public static function provideFailingOperations(): Generator
    {
        yield 'download on the stream handler' => [
            static fn (BlobRestProxy $client) => $client->getBlob('container', 'blob'),
            false,
        ];
        yield 'download on the cURL handler' => [
            static fn (BlobRestProxy $client) => $client->getBlob('container', 'blob'),
            true,
        ];
        yield 'single request upload' => [
            static fn (BlobRestProxy $client) => $client->createBlockBlob('container', 'blob', 'content'),
            false,
        ];
        yield 'block upload through ABSUploader' => [
            static function (BlobRestProxy $client): void {
                $file = (string) tempnam(sys_get_temp_dir(), 'sas-redaction');
                file_put_contents($file, 'content');
                try {
                    (new ABSUploader($client))->uploadFile('container', 'blob', $file, new CommitBlobBlocksOptions(), true);
                } finally {
                    unlink($file);
                }
            },
            false,
        ];
        yield 'empty file upload through ABSUploader' => [
            static function (BlobRestProxy $client): void {
                $file = (string) tempnam(sys_get_temp_dir(), 'sas-redaction');
                try {
                    (new ABSUploader($client))->uploadFile('container', 'blob', $file, new CreateBlockBlobOptions(), false);
                } finally {
                    unlink($file);
                }
            },
            false,
        ];
    }

    #[DataProvider('provideRejections')]
    public function testTheReplacementKeepsItsClassAndDropsTheOriginal(
        Throwable $reason,
        string $expectedClass,
        ?int $expectedStatus,
    ): void {
        $handler = (new BlobQueryRedactionMiddleware())(
            static fn (): mixed => Create::rejectionFor($reason),
        );

        try {
            $handler(self::signedRequest(), [])->wait();
            self::fail('The rejection must stay a rejection.');
        } catch (Throwable $e) {
            self::assertSame($expectedClass, $e::class);
            self::assertCarriesNoSas($e);
            if ($e instanceof RequestException) {
                self::assertSame($expectedStatus, $e->getResponse()?->getStatusCode());
            }
        }
    }

    public static function provideRejections(): Generator
    {
        $request = self::signedRequest();
        $url = (string) $request->getUri();
        $leakingPrevious = new RuntimeException('failed for ' . $url);

        yield 'transfer cut mid body keeps its response' => [
            new RequestException('cURL error 18 for ' . $url, $request, new Response(200), null, ['url' => $url]),
            RequestException::class,
            200,
        ];
        yield 'bad response' => [
            new ServerException('Server error: `GET ' . $url . '`', $request, new Response(503)),
            ServerException::class,
            503,
        ];
        yield 'chained original' => [
            new RequestException('on_headers failed', $request, null, $leakingPrevious, ['redirect_url' => $url]),
            RequestException::class,
            null,
        ];
        yield 'signature re-encoded on the way' => [
            new ConnectException('failed for sig=' . rawurldecode(self::SIGNATURE), $request),
            ConnectException::class,
            null,
        ];
    }

    public function testAFailureWithoutAQueryStringIsLeftAlone(): void
    {
        $reason = new ConnectException('refused', new Request('GET', 'http://127.0.0.1:1/container/blob'));
        $handler = (new BlobQueryRedactionMiddleware())(static fn (): mixed => Create::rejectionFor($reason));

        try {
            $handler(new Request('GET', 'http://127.0.0.1:1/container/blob'), [])->wait();
            self::fail('The rejection must stay a rejection.');
        } catch (ConnectException $e) {
            self::assertSame($reason, $e);
        }
    }

    /**
     * @param Closure(string): BlobRestProxy $factory
     */
    #[DataProvider('provideFactories')]
    public function testEveryFactoryClientRedacts(Closure $factory): void
    {
        $middlewares = $factory(self::CONNECTION_STRING)->getMiddlewares();

        // the last one pushed is the one next to Guzzle, so the retry middleware sees the redacted failure
        self::assertInstanceOf(BlobQueryRedactionMiddleware::class, end($middlewares));
    }

    public static function provideFactories(): Generator
    {
        yield 'download client' => [BlobClientFactory::createDownloadClient(...)];
        yield 'upload client' => [BlobClientFactory::createClientFromConnectionString(...)];
    }

    private static function signedRequest(): Request
    {
        return new Request(
            'GET',
            'http://127.0.0.1:1/container/blob?comp=block&sv=2020-08-04&sp=rl&sig=' . self::SIGNATURE,
        );
    }

    private static function assertCarriesNoSas(Throwable $e): void
    {
        self::assertNull($e->getPrevious(), 'a logger walking the chain would print the original');

        $carriers = [
            'message' => $e->getMessage(),
            'string cast' => (string) $e,
        ];
        if ($e instanceof RequestException || $e instanceof ConnectException) {
            $carriers['handler context'] = (string) json_encode($e->getHandlerContext());
            $carriers['request URI'] = (string) $e->getRequest()->getUri();
        }

        foreach ($carriers as $name => $carrier) {
            foreach ([self::SIGNATURE, rawurldecode(self::SIGNATURE), 'LEAK', 'sp=rl'] as $secret) {
                self::assertStringNotContainsString($secret, $carrier, $name);
            }
        }
    }
}
