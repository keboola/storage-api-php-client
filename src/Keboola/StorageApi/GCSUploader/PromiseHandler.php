<?php

namespace Keboola\StorageApi\GCSUploader;

use Google\Cloud\Core\Exception\ServiceException;

class PromiseHandler
{
    public static function getRejected(array $results): array
    {
        $rejected = [];
        foreach ($results as $filePath => $uploadInfo) {
            if ($uploadInfo['state'] === 'rejected') {
                if ($uploadInfo['reason'] instanceof ServiceException) {
                    $rejected[$filePath] = $uploadInfo['reason'];
                } else {
                    throw new \UnexpectedValueException('Not an instance of S3MultipartUploadException');
                }
            }
        }
        return $rejected;
    }
}
