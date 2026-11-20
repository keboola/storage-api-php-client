<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Downloader;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Exception\TransferException;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Query;
use Psr\Http\Message\RequestInterface;
use Throwable;

/**
 * Replaces a failed blob request's exception with one that no longer carries the query string,
 * which is where Azure puts the whole SAS.
 *
 * @internal Not part of the published package API.
 */
final class BlobQueryRedactionMiddleware
{
    private const SIGNATURE_PARAMETER = 'sig';

    /**
     * @return callable(RequestInterface, array<mixed>): PromiseInterface
     */
    public function __invoke(callable $handler): callable
    {
        return static fn (RequestInterface $request, array $options): PromiseInterface
            => $handler($request, $options)->then(
                null,
                static fn (mixed $reason): PromiseInterface => Create::rejectionFor(
                    $reason instanceof Throwable ? self::redact($reason, $request) : $reason,
                ),
            );
    }

    private static function redact(Throwable $reason, RequestInterface $sentRequest): Throwable
    {
        $request = $reason instanceof RequestException || $reason instanceof ConnectException
            ? $reason->getRequest()
            : $sentRequest;
        $secrets = self::secrets($request);
        if ($secrets === []) {
            return $reason;
        }

        $message = str_replace($secrets, '', $reason->getMessage());
        $request = $request->withUri($request->getUri()->withQuery(''));

        // The original is deliberately not chained: a logger walking getPrevious() would print it.
        if ($reason instanceof ConnectException) {
            return new ConnectException(
                $message,
                $request,
                null,
                self::redactContext($reason->getHandlerContext(), $secrets),
            );
        }
        if ($reason instanceof RequestException) {
            return new ($reason::class)(
                $message,
                $request,
                $reason->getResponse(),
                null,
                self::redactContext($reason->getHandlerContext(), $secrets),
            );
        }

        for ($e = $reason; $e !== null; $e = $e->getPrevious()) {
            if (str_replace($secrets, '', $e->getMessage()) !== $e->getMessage()) {
                return new TransferException($message, (int) $reason->getCode());
            }
        }

        return $reason;
    }

    /**
     * @return list<string>
     */
    private static function secrets(RequestInterface $request): array
    {
        $query = $request->getUri()->getQuery();
        if ($query === '') {
            return [];
        }

        $secrets = ['?' . $query, $query];
        // Catches the signature where the URL was re-encoded or reordered on the way.
        $signature = Query::parse($query, false)[self::SIGNATURE_PARAMETER] ?? null;
        if (is_string($signature) && $signature !== '') {
            $secrets[] = $signature;
            $secrets[] = rawurldecode($signature);
        }

        return $secrets;
    }

    /**
     * @param array<mixed> $context
     * @param list<string> $secrets
     * @return array<mixed>
     */
    private static function redactContext(array $context, array $secrets): array
    {
        array_walk_recursive($context, static function (mixed &$value) use ($secrets): void {
            if (is_string($value)) {
                $value = str_replace($secrets, '', $value);
            }
        });

        return $context;
    }
}
