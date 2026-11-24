<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use Google\Cloud\Core\Exception\ServiceException as GoogleServiceException;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Exception\TransferException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Throwable;

/**
 * Turns a file storage download that failed on the transport (no connection, timeout, stalled or
 * cut transfer) into a ClientException, whichever provider SDK reported it.
 *
 * @internal Not part of the published package API.
 */
final class TransportFailure
{
    public const STRING_CODE = 'downloadTransportFailure';

    /**
     * @param array<mixed> $file the getFile() response of the downloaded file
     */
    public static function translate(Throwable $failure, array $file): Throwable
    {
        $detail = self::detail($failure);
        if ($detail === null) {
            return $failure;
        }

        // The original is deliberately not chained: the provider SDK exceptions carry the request URL.
        return new ClientException(
            sprintf(
                'Cannot download file "%s" (%s): the connection to the file storage failed (%s).',
                $file['name'] ?? '',
                $file['id'] ?? '',
                $detail,
            ),
            0,
            null,
            self::STRING_CODE,
            ['provider' => $file['provider'] ?? null],
        );
    }

    private static function detail(Throwable $failure): ?string
    {
        for ($link = $failure; $link !== null; $link = self::cause($link)) {
            if ($link instanceof Exception && $link->getStringCode() === BlobDownloadWriter::INCOMPLETE_DOWNLOAD) {
                return 'the transfer ended before the whole file arrived';
            }
            if (!$link instanceof TransferException) {
                continue;
            }

            $hasContext = $link instanceof RequestException || $link instanceof ConnectException;
            $context = $hasContext ? $link->getHandlerContext() : [];
            $errno = (int) ($context['errno'] ?? 0);
            if ($errno !== 0) {
                return self::curlDetail($errno, (string) ($context['error'] ?? ''));
            }
            if ($link instanceof ConnectException
                || ($link instanceof RequestException && $link->getResponse() === null)
            ) {
                return 'no response';
            }
        }

        return null;
    }

    private static function cause(Throwable $link): ?Throwable
    {
        // the Google SDK keeps the Guzzle exception outside the previous chain
        return $link->getPrevious()
            ?? ($link instanceof GoogleServiceException ? $link->getServiceException() : null);
    }

    private static function curlDetail(int $errno, string $error): string
    {
        // cURL's own text is kept only when it cannot hold a query string
        if ($error === '' || preg_match('/[?&=]/', $error) === 1) {
            return sprintf('cURL error %d', $errno);
        }

        return sprintf('cURL error %d: %s', $errno, rtrim($error, '.'));
    }
}
