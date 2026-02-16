<?php

namespace Keboola\StorageApi\S3Uploader;

use Aws\S3\Exception\S3MultipartUploadException;

class PromiseResultHandler
{
    /**
     * @param $results
     * @return array
     */
    public static function getRejected($results)
    {
        $rejected = [];
        foreach ($results as $filePath => $uploadInfo) {
            if ($uploadInfo['state'] === 'rejected') {
                if ($uploadInfo['reason'] instanceof S3MultipartUploadException) {
                    /** @var S3MultipartUploadException $reason */
                    $rejected[$filePath] = $uploadInfo['reason'];
                } else {
                    throw new \UnexpectedValueException('Not an instance of S3MultipartUploadException');
                }
            }
        }
        return $rejected;
    }
}
