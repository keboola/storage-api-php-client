<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use Aws\Command;
use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use Closure;
use Generator;
use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\Storage\StorageClient;
use GuzzleHttp\Exception\ClientException as GuzzleClientException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Downloader\AbsDownloader;
use Keboola\StorageApi\Downloader\BlobDownloadWriter;
use Keboola\StorageApi\Downloader\BlobQueryRedactionMiddleware;
use Keboola\StorageApi\Downloader\DownloaderInterface;
use Keboola\StorageApi\Downloader\FailureTranslatingDownloader;
use Keboola\StorageApi\Downloader\GcsDownloader;
use Keboola\StorageApi\Downloader\S3ClientFactory;
use Keboola\StorageApi\Downloader\S3Downloader;
use Keboola\StorageApi\Downloader\TransportFailure;
use Keboola\StorageApi\Exception;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;

/**
 * Decision table of what counts as a transport failure, and the guard that the translated
 * exception carries nothing of the provider SDK's one.
 */
class TransportFailureTest extends TestCase
{
    private const CLOSED_PORT = 'http://127.0.0.1:1';

    private const SECRET = 'LEAKMARKER';

    private const FILE = [
        'id' => 123,
        'name' => 'data.csv',
        's3Path' => ['bucket' => 'bucket', 'key' => 'exp/data.csv'],
        'absPath' => ['container' => 'container', 'name' => 'data.csv'],
        'gcsPath' => ['bucket' => 'bucket', 'key' => 'exp/data.csv'],
    ];

    /**
     * @param Closure(): DownloaderInterface $downloader
     */
    #[DataProvider('provideDownloadersOnAClosedPort')]
    public function testAnUnreachableFileStorageRaisesAClientException(Closure $downloader, string $provider): void
    {
        $destination = sys_get_temp_dir() . '/' . uniqid('transport-failure-', true);

        try {
            (new FailureTranslatingDownloader($downloader()))
                ->downloadFileFromFileResponse(self::FILE + ['provider' => $provider], $destination);
            self::fail('The download from a closed port must fail.');
        } catch (ClientException $e) {
            self::assertSame(TransportFailure::STRING_CODE, $e->getStringCode());
            self::assertStringStartsWith(
                'Cannot download file "data.csv" (123): the connection to the file storage failed (',
                $e->getMessage(),
            );
            self::assertSame(['provider' => $provider], $e->getContextParams());
            self::assertNull($e->getPrevious(), 'the SDK exception carries the request URL');
            self::assertStringNotContainsString(self::SECRET, (string) $e);
            self::assertStringNotContainsString('127.0.0.1:1/', (string) $e);
        } finally {
            @unlink($destination);
        }
    }

    public static function provideDownloadersOnAClosedPort(): Generator
    {
        yield 'azure' => [
            static function (): DownloaderInterface {
                $client = BlobRestProxy::createBlobService(
                    sprintf('BlobEndpoint=%s;SharedAccessSignature=sv=2020-08-04&sig=%s', self::CLOSED_PORT, self::SECRET),
                    ['http' => ['connect_timeout' => 1]],
                );
                $client->pushMiddleware(new BlobQueryRedactionMiddleware());

                return new AbsDownloader($client);
            },
            'azure',
        ];
        yield 'aws' => [
            static fn (): DownloaderInterface => new S3Downloader(new S3Client(
                [
                    'region' => 'eu-central-1',
                    'endpoint' => self::CLOSED_PORT,
                    'use_path_style_endpoint' => true,
                    'credentials' => ['key' => 'key', 'secret' => self::SECRET, 'token' => self::SECRET],
                ] + S3ClientFactory::transferOptions(0),
            )),
            'aws',
        ];
        yield 'gcp' => [
            static fn (): DownloaderInterface => new GcsDownloader(new StorageClient([
                'projectId' => 'project',
                'apiEndpoint' => self::CLOSED_PORT,
                'retries' => 0,
                'credentialsFetcher' => new class implements FetchAuthTokenInterface {
                    /**
                     * @return array{access_token: string}
                     */
                    public function fetchAuthToken(?callable $httpHandler = null): array
                    {
                        return ['access_token' => 'LEAKMARKER'];
                    }

                    public function getCacheKey(): string
                    {
                        return '';
                    }

                    /**
                     * @return array{access_token: string}
                     */
                    public function getLastReceivedToken(): array
                    {
                        return ['access_token' => 'LEAKMARKER'];
                    }
                },
            ])),
            'gcp',
        ];
    }

    #[DataProvider('provideFailures')]
    public function testOnlyATransportFailureIsTranslated(Throwable $failure, ?string $expectedDetail): void
    {
        $translated = TransportFailure::translate($failure, self::FILE);

        if ($expectedDetail === null) {
            self::assertSame($failure, $translated);

            return;
        }
        self::assertInstanceOf(ClientException::class, $translated);
        self::assertNull($translated->getPrevious());
        self::assertSame(
            sprintf('Cannot download file "data.csv" (123): the connection to the file storage failed (%s).', $expectedDetail),
            $translated->getMessage(),
        );
    }

    public static function provideFailures(): Generator
    {
        $request = new Request('GET', 'https://account.blob.core.windows.net/c/b?sig=' . self::SECRET);
        $stalled = new RequestException('stalled', $request, null, null, [
            'errno' => 28,
            'error' => 'Operation too slow. Less than 1024 bytes/sec transferred the last 60 seconds',
        ]);
        $cutMidBody = new RequestException('cut', $request, new Response(200), null, [
            'errno' => 18,
            'error' => 'transfer closed for https://host/c/b?sig=' . self::SECRET,
        ]);
        $forbidden = new GuzzleClientException('forbidden', $request, new Response(403));

        yield 'stalled transfer' => [
            $stalled,
            'cURL error 28: Operation too slow. Less than 1024 bytes/sec transferred the last 60 seconds',
        ];
        yield 'cURL text that could hold a query string is dropped' => [$cutMidBody, 'cURL error 18'];
        yield 'no response without cURL context' => [new RequestException('refused', $request), 'no response'];
        yield 'wrapped by the AWS SDK' => [
            new S3Exception('Error executing "GetObject"', new Command('GetObject'), [], $stalled),
            'cURL error 28: Operation too slow. Less than 1024 bytes/sec transferred the last 60 seconds',
        ];
        yield 'incomplete Azure blob' => [
            new Exception('incomplete', null, null, BlobDownloadWriter::INCOMPLETE_DOWNLOAD),
            'the transfer ended before the whole file arrived',
        ];
        yield 'HTTP error response is not a transport failure' => [$forbidden, null];
        yield 'missing S3 object' => [
            new S3Exception('NoSuchKey', new Command('GetObject'), ['code' => 'NoSuchKey'], $forbidden),
            null,
        ];
        yield 'missing GCS object' => [new NotFoundException('not found', 404, $forbidden), null];
        yield 'Azure service error' => [new ServiceException(new Response(403)), null];
        yield 'file not found reported by the client' => [new ClientException('not found', 404, $forbidden), null];
    }
}
