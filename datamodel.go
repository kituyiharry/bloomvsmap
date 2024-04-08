package main

type Model struct {
	Id        string `json:"id"`
	Type      string `json:"type"`
	Public    bool   `json:"public"`
	CreatedAt string `json:"created_at"`
	Actor     struct {
		Id     int    `json:"id"`
		Login  string `json:"login"`
		Grav   string `json:"gravatar_id"`
		Url    string `json:"url"`
		Avatar string `json:"avatar_url"`
	} `json:"actor"`
	Repo struct {
		Id   int    `json:"id"`
		Name string `json:"name"`
		Url  string `json:"url"`
	} `json:"repo"`
	Payload struct {
		Action       string `json:"action"`
		Ref          string `json:"ref"`
		RefType      string `json:"ref_type"`
		MasterBranch string `json:"master_branch"`
		Description  string `json:"description"`
		PusherType   string `json:"pusher_type"`
		Head         string `json:"head"`
		Before       string `json:"before"`
		Commits      struct {
			Sha    string `json:"sha"`
			Author struct {
				Email string `json:"email"`
				Name  string `json:"name"`
			} `json:"author"`
			Message  string `json:"message"`
			Distinct bool   `json:"distinct"`
			Url      string `json:"url"`
		} `json:"commits"`
	} `json:"payload"`
}
