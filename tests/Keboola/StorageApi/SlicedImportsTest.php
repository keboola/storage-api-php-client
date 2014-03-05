<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */


use Keboola\StorageApi\Client;

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_SlicedImportsTest extends StorageApiTestCase
{

	protected $_inBucketId;
	protected $_outBucketId;


	public function setUp()
	{
		parent::setUp();

		$this->_outBucketId = $this->_initEmptyBucket('api-tests', 'out');
		$this->_inBucketId = $this->_initEmptyBucket('api-tests', 'in');
	}

	public function testSlicedImportGzipped()
	{

		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('entries_')
			->setIsSliced(true);
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$part1URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001.gz',
			'Body'   => fopen(__DIR__ . '/_data/sliced/neco_0000_part_00.gz', 'r+'),
		))->get('ObjectURL');

		$part2URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part002.gz',
			'Body'   => fopen(__DIR__ . '/_data/sliced/neco_0001_part_00.gz', 'r+'),
		))->get('ObjectURL');

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode(array(
				'entries' => array(
					array(
						'url' => $part1URL,
					),
					array(
						'url' => $part2URL,
					)
				),
			)),
		))->get('ObjectURL');

		$headerFile = new CsvFile(__DIR__ . '/_data/sliced/header.csv');
		$tableId = $this->_client->createTable($this->_inBucketId, 'entries', $headerFile);
		$this->_client->writeTableAsyncDirect($tableId, array(
			'dataFileId' => $slicedFile['id'],
			'columns' => $headerFile->getHeader(),
			'delimiter' => '|',
			'enclosure' => '',
			'escapedBy' => '',
		));
	}

	public function testSlicedImportSingleFile()
	{
		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('languages_')
			->setIsSliced(true);
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$part1URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001.csv',
			'Body'   => fopen(__DIR__ . '/_data/languages.csv', 'r+'),
		))->get('ObjectURL');

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode(array(
				'entries' => array(
					array(
						'url' => $part1URL,
					),
				),
			)),
		))->get('ObjectURL');

		$tableId = $this->_client->createTable($this->_inBucketId, 'entries', new CsvFile(__DIR__ . '/_data/languages.csv'));
		$this->_client->deleteTableRows($tableId);
		$this->_client->writeTableAsyncDirect($tableId, array(
			'dataFileId' => $slicedFile['id'],
			'delimiter' => ',',
			'enclosure' => '"',
			'escapedBy' => '',
		));

		$this->assertEquals(file_get_contents(__DIR__ . '/_data/languages.csv'), $this->_client->exportTable($tableId, null, array(
			'format' => 'rfc',
		)), 'imported data comparsion');

		// incremental
		$this->_client->writeTableAsyncDirect($tableId,  array(
			'dataFileId' => $slicedFile['id'],
			'incremental' => true,
			'delimiter' => ',',
			'enclosure' => '"',
			'escapedBy' => '',
		));

		$data = file_get_contents(__DIR__ . '/_data/languages.csv');
		$lines = explode("\n", $data);
		array_shift($lines);
		$data = $data . implode("\n", $lines);

		$this->assertEquals($data, $this->_client->exportTable($tableId, null, array(
			'format' => 'rfc',
		)), 'imported data comparsion');
	}

	public function testSlicedImportMissingManifest()
	{
		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('entries_')
			->setIsSliced(true);
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$tableId = $this->_client->createTable($this->_inBucketId, 'entries', new CsvFile(__DIR__ . '/_data/sliced/header.csv'));

		try {
			$this->_client->writeTableAsyncDirect($tableId, array(
				'dataFileId' => $slicedFile['id'],
				'withoutHeaders' => true,
				'delimiter' => '|',
				'enclosure' => '',
				'escapedBy' => '',
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.manifestFileMissing', $e->getStringCode());
		}
	}

	public function testInvalidFilesInManifest()
	{
		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('entries_')
			->setIsSliced(true);
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$tableId = $this->_client->createTable($this->_inBucketId, 'entries', new CsvFile(__DIR__ . '/_data/sliced/header.csv'));

		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$part1URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001.gz',
			'Body'   => fopen(__DIR__ . '/_data/sliced/neco_0000_part_00.gz', 'r+'),
		))->get('ObjectURL');

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode(array(
				'entries' => array(
					array(
						'url' => $part1URL,
					),
					array(
						'url' => $part1URL . 'some',
					)
				),
			)),
		))->get('ObjectURL');

		try {
			$this->_client->writeTableAsyncDirect($tableId, array(
				'dataFileId' => $slicedFile['id'],
				'withoutHeaders' => true,
				'delimiter' => '|',
				'enclosure' => '',
				'escapedBy' => '',
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.importFileMissing', $e->getStringCode());
		}
	}

	public function testUnauthorizedAccessInManifestFile()
	{
		$tableId = $this->_client->createTable($this->_inBucketId, 'entries', new CsvFile(__DIR__ . '/_data/sliced/header.csv'));

		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('entries_')
			->setIsSliced(true);

		// First upload
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$part1URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001.gz',
			'Body'   => fopen(__DIR__ . '/_data/escaping.csv', 'r+'),
		))->get('ObjectURL');


		// Second upload
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);
		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);
		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode(array(
				'entries' => array(
					array(
						'url' => $part1URL,
					),
				),
			)),
		))->get('ObjectURL');

		try {
			$this->_client->writeTableAsyncDirect($tableId, array(
				'dataFileId' => $slicedFile['id'],
				'withoutHeaders' => true,
				'delimiter' => '|',
				'enclosure' => '',
				'escapedBy' => '',
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.unauthorizedAccess', $e->getStringCode());
		}
	}

}