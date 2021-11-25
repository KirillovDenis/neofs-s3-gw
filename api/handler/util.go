package handler

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neofs-s3-gw/api"
	"github.com/nspcc-dev/neofs-s3-gw/api/errors"
	"github.com/nspcc-dev/neofs-s3-gw/api/layer"
	"go.uber.org/zap"
)

func (h *handler) logAndSendError(w http.ResponseWriter, logText string, reqInfo *api.ReqInfo, err error, additional ...zap.Field) {
	fields := []zap.Field{zap.String("request_id", reqInfo.RequestID),
		zap.String("method", reqInfo.API),
		zap.String("bucket_name", reqInfo.BucketName),
		zap.String("object_name", reqInfo.ObjectName),
		zap.Error(err)}
	fields = append(fields, additional...)

	h.log.Error(logText, fields...)
	api.WriteErrorResponse(w, reqInfo, err)
}

func (h *handler) checkBucketOwner(r *http.Request, bucket string, header ...string) error {
	var expected string
	if len(header) == 0 {
		expected = r.Header.Get(api.AmzExpectedBucketOwner)
	} else {
		expected = header[0]
	}

	if len(expected) == 0 {
		return nil
	}

	bktInfo, err := h.obj.GetBucketInfo(r.Context(), bucket)
	if err != nil {
		return err
	}

	return checkOwner(bktInfo, expected)
}

func parseRange(s string) (*layer.RangeParams, error) {
	if s == "" {
		return nil, nil
	}

	prefix := "bytes="

	if !strings.HasPrefix(s, prefix) {
		return nil, errors.GetAPIError(errors.ErrInvalidRange)
	}

	s = strings.TrimPrefix(s, prefix)

	valuesStr := strings.Split(s, "-")
	if len(valuesStr) != 2 {
		return nil, errors.GetAPIError(errors.ErrInvalidRange)
	}

	values := make([]uint64, 0, len(valuesStr))
	for _, v := range valuesStr {
		num, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, errors.GetAPIError(errors.ErrInvalidRange)
		}
		values = append(values, num)
	}
	if values[0] > values[1] {
		return nil, errors.GetAPIError(errors.ErrInvalidRange)
	}

	return &layer.RangeParams{
		Start: values[0],
		End:   values[1],
	}, nil
}
