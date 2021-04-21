<?php

header('Content-Type: text/html; charset=utf-8');
ini_set('max_execution_time', 480);
ini_set('display_errors', 1);
ini_set('memory_limit', '1024M');
date_default_timezone_set('America/Lima');
require __DIR__ . '/vendor/autoload.php';

use Google\Cloud\Storage\StorageClient;
use Google\Cloud\BigQuery\BigQueryClient;

echo "<pre>";

//if (php_sapi_name() != 'cli') {
//    throw new Exception('This application must be run on the command line.');
//}

/**
 * Returns an authorized API client.
 * @return Google_Client the authorized client object
 */
function getClient() {


    // Get Google Client

    $client = new Google_Client();
    $client->setApplicationName('Gmail API PHP Quickstart');
    $client->setScopes(Google_Service_Gmail::GMAIL_READONLY);
    $client->setAuthConfig('credentials.json');
    //$client->setAuthConfig('../htdocs/gmail_api/credentials.json');
    $client->setAccessType('offline');
    $client->setPrompt('select_account consent');


    // Load previously authorized token from a file, if it exists.
    // The file token.json stores the user's access and refresh tokens, and is
    // created automatically when the authorization flow completes for the first
    // time.

    $tokenPath = 'token.json';
    if (file_exists($tokenPath)) {
        $accessToken = json_decode(file_get_contents($tokenPath), true);
        $client->setAccessToken($accessToken);
    }


    // If there is no previous token or it's expired.

    if ($client->isAccessTokenExpired()) {


        // Refresh the token if possible, else fetch a new one.

        if ($client->getRefreshToken()) {
            $client->fetchAccessTokenWithRefreshToken($client->getRefreshToken());
        } else {


            // Request authorization from the user.

            $authUrl = $client->createAuthUrl();
            printf("Open the following link in your browser:\n%s\n", $authUrl);
            print 'Enter verification code: ';
            $authCode = trim(fgets(STDIN));


            // Exchange authorization code for an access token.

            $accessToken = $client->fetchAccessTokenWithAuthCode($authCode);
            $client->setAccessToken($accessToken);


            // Check to see if there was an error.

            if (array_key_exists('error', $accessToken)) {
                throw new Exception(join(', ', $accessToken));
            }
        }


        // Save the token to a file.

        if (!file_exists(dirname($tokenPath))) {
            mkdir(dirname($tokenPath), 0700, true);
        }
        file_put_contents($tokenPath, json_encode($client->getAccessToken()));
    }
    return $client;
}

$client = getClient();
$service = new Google_Service_Gmail($client);
$user = 'me';
$opt_param['maxResults'] = 1;
$opt_param['q'] = "subject:([Botmaker Internal] Reporte Ecommerce)";
$results = $service->users_messages->listUsersMessages($user, $opt_param);
$attachment = [];

if (count($results->getMessages()) == 1) {


    // get attachment

    $messageId = $results->getMessages()[0]->getId();
    $message = $service->users_messages->get($user, $messageId);
    $messageParts = $message->payload->parts;
    $attachments = [];
    foreach ($messageParts as $part) {
        if (!empty($part->body->attachmentId)) {
            $attachment = $service->users_messages_attachments->get('me', $messageId, $part->body->attachmentId);
            $attachments[] = [
                'filename' => $part->filename,
                'mimeType' => $part->mimeType,
                'data' => strtr($attachment->data, '-_', '+/')
            ];
        }
    }


    // fill data

    $data = array();
    foreach (explode("\n", base64_decode($attachments[0]['data'])) as $row) {
        $row_array = str_getcsv($row);


        // queues

        $colas = ["ventasc2ws2s", "ventasc2w", "leads", "gsem-2w",
            "gpromo-2w", "gdis-2w", "c2w-cvm", "c2whogar"];
        for ($j = 0; $j < count($colas); $j++) {
            if (count($row_array) >= 8) {
                if ($colas[$j] == $row_array[8]) {
                    $element[0] = $row_array[6];
                    $element[1] = $row_array[1];
                    $element[2] = str_replace("-", "", $row_array[0]);
                    $element[3] = $row_array[4];
                    $element[4] = $row_array[7] == "true" ? 1 : 0;
                    $element[5] = $row_array[3];
                    $element[6] = $row_array[8];
                    $data[] = $element;
                }
            }
        }
    }


    // get dates

    $fechas = array();
    for ($i = 0; $i < count($data); $i++) {
        $row = $data[$i];
        $fechas[] = substr($row[2], 0, 10);
    }
    sort($fechas);
    $desde = $fechas[0];
    $hasta = $fechas[count($fechas) - 1];


    // save csv

    $archivo_final = "whatsapp_" . $hasta . ".csv";
    $out = fopen($archivo_final, 'w');
    foreach ($data as $row) {
        fputcsv($out, $row);
    }
    fclose($out);


    // subir csv

    if (count($data) > 0) {


        // csv a cloud storage

        $storage = new StorageClient([
            'projectId' => "entel-ecommerce",
            'keyFilePath' => "entel-ecommerce-5b11ddb572e4.json"
        ]);
        $bucket = $storage->bucket('entel-ecommerce-bucket');
        $bucket->upload(fopen($archivo_final, 'r'));


        // bigquery instanciar objeto

        $bigQuery = new BigQueryClient([
            'projectId' => "entel-ecommerce",
            'keyFilePath' => "entel-ecommerce-5b11ddb572e4.json"
        ]);


        // bigquery borrar data

        $queryJobConfig = $bigQuery->query('
                DELETE FROM `entel-ecommerce.entel_ds_ecommerce.whatsapp`
                WHERE
                    Session_Date BETWEEN "' . $desde . '" AND "' . $hasta . '"
            ');
        $bigQuery->runQuery($queryJobConfig);


        // bigquery importar de cloustorage

        $dataset = $bigQuery->dataset("entel_ds_ecommerce");
        $table = $dataset->table("whatsapp");
        $object = $bucket->object($archivo_final);
        $loadJobConfig = $table->loadFromStorage($object);
        $table->runJob($loadJobConfig);
        echo "Archivo subido correctamente a BigQuery.<br>"
        . "Cantidad de filas subidas: " . count($data) . ".<br>"
        . "Desde: " . $desde . ".<br>"
        . "Hasta: " . $hasta . ".<br>";


        // bigquery ultima actualizacion

        $queryJobConfig_2 = $bigQuery->query('
                INSERT INTO `entel-ecommerce.entel_ds_ecommerce.ultimas_actualizaciones` (
                    tabla,
                    ultima_actualizacion
                )
                VALUES (
                    "whatsapp",
                    "' . date("Y-m-d H:i:s") . '"
                )
            ');
        $bigQuery->runQuery($queryJobConfig_2);


        // borra archivo

        if (file_exists($archivo_final)) {
            unlink($archivo_final);
        }
    } else {
        echo "Sin data por subir.<br>";
    }
}