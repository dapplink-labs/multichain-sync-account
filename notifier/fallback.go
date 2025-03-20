package notifier

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	gresty "github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
)

var errBlockChainHTTPError = errors.New("blockchain http error")

type NotifyClient struct {
	client *gresty.Client
}

func NewNotifierClient(baseUrl string) (*NotifyClient, error) {
	if baseUrl == "" {
		return nil, fmt.Errorf("blockchain URL cannot be empty")
	}
	client := gresty.New()
	client.SetBaseURL(baseUrl)
	client.OnAfterResponse(func(c *gresty.Client, r *gresty.Response) error {
		statusCode := r.StatusCode()
		if statusCode >= 400 {
			method := r.Request.Method
			url := r.Request.URL
			return fmt.Errorf("%d cannot %s %s: %w", statusCode, method, url, errBlockChainHTTPError)
		}
		return nil
	})
	return &NotifyClient{
		client: client,
	}, nil
}

func (nc *NotifyClient) BusinessNotify(notifyData *NotifyRequest) (bool, error) {
	body, err := json.Marshal(notifyData)
	if err != nil {
		log.Error("failed to marshal notify data", "err", err)
		return false, err
	}
	res, err := nc.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(body).
		SetResult(&NotifyResponse{}).Post("/dapplink/notify")
	if err != nil {
		log.Error("get transaction fee fail", "err", err)
		return false, err
	}
	spt, ok := res.Result().(*NotifyResponse)
	if !ok {
		return false, errors.New("get transaction fee fail, ok is false")
	}
	return spt.Success, nil
}
